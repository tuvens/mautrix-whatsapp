// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2026 Tulir Asokan and contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

const (
	newsletterInitialPollDelay = 2 * time.Minute
	newsletterMaxPollPages     = 100
	newsletterMaxPollItems     = 1000
	newsletterMaxPollDuration  = 30 * time.Second
	newsletterMaxPending       = 1000
	newsletterMinRetryDelay    = time.Minute
	newsletterMaxRetryDelay    = time.Hour
	newsletterWatermarkVersion = 1
)

type newsletterAPI interface {
	GetSubscribedNewsletters(context.Context) ([]*types.NewsletterMetadata, error)
	GetNewsletterMessages(context.Context, types.JID, *whatsmeow.GetNewsletterMessagesParams) ([]*types.NewsletterMessage, error)
	NewsletterSubscribeLiveUpdates(context.Context, types.JID) (time.Duration, error)
}

type newsletterPollDeliveryContextKey struct{}

func newsletterDeliverySource(ctx context.Context) string {
	if fromPoll, _ := ctx.Value(newsletterPollDeliveryContextKey{}).(bool); fromPoll {
		return "poll"
	}
	return "push"
}

func (wa *WhatsAppClient) startNewsletterReliabilityLoop() {
	wa.stopNewsletterReliabilityLoop()
	cfg := &wa.Main.Config.NewsletterReliability
	if !cfg.EnableBackfillPoll && !cfg.EnableLiveUpdates {
		return
	}
	ctx, cancel := context.WithCancel(wa.Main.Bridge.BackgroundCtx)
	wa.stopNewsletterReliability.Store(&cancel)
	ctx = wa.UserLogin.Log.WithContext(ctx)
	go wa.newsletterReliabilityLoop(ctx, wa.Client)
}

func (wa *WhatsAppClient) stopNewsletterReliabilityLoop() {
	if stop := wa.stopNewsletterReliability.Swap(nil); stop != nil {
		(*stop)()
	}
}

func (wa *WhatsAppClient) newsletterReliabilityLoop(ctx context.Context, api newsletterAPI) {
	cfg := &wa.Main.Config.NewsletterReliability
	if cfg.EnableBackfillPoll {
		go wa.newsletterBackfillLoop(ctx, api)
	}
	if cfg.EnableLiveUpdates {
		for _, jid := range cfg.liveUpdatesJIDs {
			go wa.newsletterLiveUpdatesLoop(ctx, api, jid)
		}
	}
	<-ctx.Done()
}

func waitNewsletterDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func jitterNewsletterDelay(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	span := int64(base) / 5
	if span == 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(2*span+1)-span)
}

func (wa *WhatsAppClient) newsletterBackfillLoop(ctx context.Context, api newsletterAPI) {
	workers := make(map[types.JID]context.CancelFunc)
	defer func() {
		for _, cancel := range workers {
			cancel()
		}
	}()
	retryDelay := newsletterMinRetryDelay
	for {
		newsletters, err := api.GetSubscribedNewsletters(ctx)
		if ctx.Err() != nil {
			return
		}
		var next time.Duration
		if err != nil {
			rateLimitShaped := isRateLimitShaped(err)
			if rateLimitShaped {
				next = jitterNewsletterDelay(retryDelay)
				retryDelay = min(retryDelay*2, newsletterMaxRetryDelay)
			} else {
				// A timeout or disconnect abandons this cycle and returns to
				// the ordinary low-cadence tick. Only rate-limit-shaped
				// responses increase request frequency through an explicit,
				// bounded retry schedule.
				next = jitterNewsletterDelay(wa.Main.Config.NewsletterReliability.PollInterval)
				retryDelay = newsletterMinRetryDelay
			}
			wa.UserLogin.Log.Error().Err(err).
				Bool("rate_limit_shaped", rateLimitShaped).
				Dur("next_attempt_in", next).
				Msg("Failed to refresh subscribed newsletters; active channel pollers are unchanged")
		} else {
			active := make(map[types.JID]struct{}, len(newsletters))
			for _, newsletter := range newsletters {
				if newsletter == nil || newsletter.ID.Server != types.NewsletterServer {
					continue
				}
				jid := newsletter.ID.ToNonAD()
				active[jid] = struct{}{}
				if _, exists := workers[jid]; !exists {
					workerCtx, cancel := context.WithCancel(ctx)
					workers[jid] = cancel
					go wa.newsletterChannelBackfillLoop(workerCtx, api, jid)
				}
			}
			for jid, cancel := range workers {
				if _, stillFollowed := active[jid]; !stillFollowed {
					cancel()
					delete(workers, jid)
					wa.UserLogin.Log.Info().Stringer("newsletter_jid", jid).
						Msg("Stopped reliability polling for unfollowed newsletter")
				}
			}
			next = jitterNewsletterDelay(wa.Main.Config.NewsletterReliability.PollInterval)
			retryDelay = newsletterMinRetryDelay
		}
		if !waitNewsletterDelay(ctx, next) {
			return
		}
	}
}

func (wa *WhatsAppClient) newsletterChannelBackfillLoop(ctx context.Context, api newsletterAPI, jid types.JID) {
	// Discover subscriptions immediately after connect, but spread each
	// channel's first fetch independently around the two-minute mark. A single
	// global delay followed by 51 simultaneous iqs is not per-channel jitter.
	if !waitNewsletterDelay(ctx, jitterNewsletterDelay(newsletterInitialPollDelay)) {
		return
	}
	retryDelay := newsletterMinRetryDelay
	for {
		err := wa.pollNewsletterChannel(ctx, api, jid)
		if ctx.Err() != nil {
			return
		}
		var next time.Duration
		if err != nil {
			rateLimitShaped := isRateLimitShaped(err)
			if rateLimitShaped {
				next = jitterNewsletterDelay(retryDelay)
				retryDelay = min(retryDelay*2, newsletterMaxRetryDelay)
			} else {
				// Ordinary transport failures abandon this cycle. The next
				// attempt is the next configured tick, rather than a burst of
				// retries while the connection is unhealthy.
				next = jitterNewsletterDelay(wa.Main.Config.NewsletterReliability.PollInterval)
				retryDelay = newsletterMinRetryDelay
			}
			wa.UserLogin.Log.Error().Err(err).Stringer("newsletter_jid", jid).
				Bool("rate_limit_shaped", rateLimitShaped).
				Dur("next_attempt_in", next).
				Msg("Newsletter catch-up poll failed; cycle abandoned")
		} else {
			next = jitterNewsletterDelay(wa.Main.Config.NewsletterReliability.PollInterval)
			retryDelay = newsletterMinRetryDelay
		}
		if !waitNewsletterDelay(ctx, next) {
			return
		}
	}
}

func isRateLimitShaped(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "rate") || strings.Contains(lower, "429") || strings.Contains(lower, "resource-limit")
}

func (wa *WhatsAppClient) pollNewsletterChannel(ctx context.Context, api newsletterAPI, jid types.JID) error {
	cfg := &wa.Main.Config.NewsletterReliability
	state, messages, scan, err := wa.prepareNewsletterCatchup(ctx, api, jid, cfg.PollCount)
	if err != nil {
		return fmt.Errorf("fetch newsletter messages: %w", err)
	}
	for _, message := range messages {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if message.Message == nil {
			wa.UserLogin.Log.Warn().Stringer("newsletter_jid", jid).
				Int("server_id", message.MessageServerID).
				Msg("Fetched newsletter message had no body; leaving watermark unchanged")
			return fmt.Errorf("server_id %d had no message body", message.MessageServerID)
		}
		evt := newsletterMessageEvent(jid, message)
		pollCtx := context.WithValue(ctx, newsletterPollDeliveryContextKey{}, true)
		if !wa.handleWAMessage(pollCtx, evt) {
			return fmt.Errorf("bridge rejected server_id %d", message.MessageServerID)
		}
	}
	// This deliberately uses warn: the paired bridge's logger must remain at
	// warn or stricter because info/debug can carry pairing credentials. The
	// record contains identifiers and counters only, never message payloads,
	// and is the evidence that a 72-hour soak actually ran its poll cycles.
	wa.UserLogin.Log.Warn().Stringer("newsletter_jid", jid).
		Int64("starting_watermark", state.watermark).
		Int("candidate_count", len(messages)).
		Int("history_pages", scan.pages).
		Int("history_items", scan.items).
		Bool("gap_pending", scan.pending).
		Str("bounded_by", scan.boundedBy).
		Int64("recovery_before", int64(scan.nextBefore)).
		Msg("Newsletter reliability poll audit")
	return nil
}

type newsletterCatchupState struct {
	version        int
	watermark      int64
	recoveryBefore types.MessageServerID
}

type newsletterCatchupScan struct {
	messages        []*types.NewsletterMessage
	boundaryMessage *types.NewsletterMessage
	boundaryIDs     []int64
	oldest          types.MessageServerID
	nextBefore      types.MessageServerID
	pages           int
	items           int
	pending         bool
	historyEnd      bool
	boundedBy       string
}

type newsletterCatchupBoundary func(*types.NewsletterMessage) (bool, error)

func collectNewsletterCatchup(ctx context.Context, api newsletterAPI, jid types.JID, watermark int64, count int) ([]*types.NewsletterMessage, error) {
	scan, err := scanNewsletterCatchup(
		ctx, api, jid, 0, count, newsletterMaxPollPages, newsletterMaxPollItems,
		time.Now().Add(newsletterMaxPollDuration),
		func(message *types.NewsletterMessage) (bool, error) {
			return int64(message.MessageServerID) <= watermark, nil
		},
	)
	if err != nil || scan.pending {
		return nil, err
	}
	return scan.messages, nil
}

func scanNewsletterCatchup(
	ctx context.Context,
	api newsletterAPI,
	jid types.JID,
	before types.MessageServerID,
	count int,
	maxPages int,
	maxItems int,
	deadline time.Time,
	boundary newsletterCatchupBoundary,
) (newsletterCatchupScan, error) {
	result := newsletterCatchupScan{nextBefore: before}
	if count <= 0 {
		return result, fmt.Errorf("newsletter history count must be positive")
	}
	seen := make(map[struct {
		serverID  types.MessageServerID
		messageID types.MessageID
	}]struct{})
	for {
		if result.pages >= maxPages {
			result.pending = true
			result.boundedBy = "pages"
			break
		}
		if result.items >= maxItems {
			result.pending = true
			result.boundedBy = "items"
			break
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			result.pending = true
			result.boundedBy = "time"
			break
		}
		requestCount := min(count, maxItems-result.items)
		if requestCount <= 0 {
			result.pending = true
			result.boundedBy = "items"
			break
		}
		requestCtx := ctx
		cancel := func() {}
		if !deadline.IsZero() {
			requestCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		messages, err := api.GetNewsletterMessages(requestCtx, jid, &whatsmeow.GetNewsletterMessagesParams{
			Count:  requestCount,
			Before: before,
		})
		cancel()
		if err != nil {
			if !deadline.IsZero() && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				result.pending = true
				result.boundedBy = "time"
				break
			}
			return result, err
		}
		if len(messages) > requestCount {
			return result, fmt.Errorf("newsletter history returned %d items for bounded request of %d", len(messages), requestCount)
		}
		result.pages++
		result.items += len(messages)
		if len(messages) == 0 {
			result.historyEnd = true
			break
		}
		var oldest types.MessageServerID
		reachedBoundary := false
		for _, message := range messages {
			if message == nil || message.MessageServerID <= 0 {
				continue
			}
			if oldest == 0 || message.MessageServerID < oldest {
				oldest = message.MessageServerID
			}
			atBoundary, err := boundary(message)
			if err != nil {
				return result, err
			}
			if atBoundary {
				if result.boundaryMessage == nil || message.MessageServerID < result.boundaryMessage.MessageServerID {
					result.boundaryMessage = message
				}
				result.boundaryIDs = append(result.boundaryIDs, int64(message.MessageServerID))
				reachedBoundary = true
				continue
			}
			key := struct {
				serverID  types.MessageServerID
				messageID types.MessageID
			}{message.MessageServerID, message.MessageID}
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				result.messages = append(result.messages, message)
			}
		}
		if oldest == 0 {
			return result, fmt.Errorf("newsletter history page contained no usable server IDs")
		}
		result.oldest = oldest
		result.nextBefore = oldest
		if reachedBoundary {
			break
		}
		if len(messages) < requestCount {
			result.historyEnd = true
			break
		}
		if oldest == 0 || oldest == before {
			return result, fmt.Errorf("newsletter pagination did not advance before=%d", oldest)
		}
		before = oldest
	}
	slices.SortFunc(result.messages, func(a, b *types.NewsletterMessage) int {
		return cmp.Compare(a.MessageServerID, b.MessageServerID)
	})
	return result, nil
}

func newsletterMessageEvent(jid types.JID, message *types.NewsletterMessage) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: jid, Sender: jid},
			ID:            message.MessageID,
			ServerID:      message.MessageServerID,
			Timestamp:     message.Timestamp,
		},
		Message: message.Message,
	}
}

func (wa *WhatsAppClient) getNewsletterWatermark(ctx context.Context, jid types.JID) (int64, error) {
	wa.newsletterWatermarkLock.Lock()
	defer wa.newsletterWatermarkLock.Unlock()
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return 0, err
	}
	return portal.Metadata.(*waid.PortalMetadata).LastNewsletterServerID, nil
}

func (wa *WhatsAppClient) getNewsletterCatchupState(ctx context.Context, jid types.JID) (newsletterCatchupState, error) {
	wa.newsletterWatermarkLock.Lock()
	defer wa.newsletterWatermarkLock.Unlock()
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return newsletterCatchupState{}, err
	}
	meta := portal.Metadata.(*waid.PortalMetadata)
	recoveryBefore := types.MessageServerID(meta.NewsletterRecoveryBefore)
	if meta.NewsletterRecoveryBefore < 0 {
		recoveryBefore = 0
	}
	return newsletterCatchupState{
		version:        meta.NewsletterWatermarkVersion,
		watermark:      meta.LastNewsletterServerID,
		recoveryBefore: recoveryBefore,
	}, nil
}

func (wa *WhatsAppClient) prepareNewsletterCatchup(
	ctx context.Context,
	api newsletterAPI,
	jid types.JID,
	count int,
) (newsletterCatchupState, []*types.NewsletterMessage, newsletterCatchupScan, error) {
	state, err := wa.getNewsletterCatchupState(ctx, jid)
	if err != nil {
		return state, nil, newsletterCatchupScan{}, fmt.Errorf("load watermark state: %w", err)
	}
	legacyRecovery := state.version < newsletterWatermarkVersion && state.watermark > 0
	initialHistory := state.version < newsletterWatermarkVersion && state.watermark == 0
	boundary := newsletterCatchupBoundary(func(message *types.NewsletterMessage) (bool, error) {
		return int64(message.MessageServerID) <= state.watermark, nil
	})
	if legacyRecovery {
		boundary = func(message *types.NewsletterMessage) (bool, error) {
			return wa.isNewsletterMessageDurable(ctx, jid, message)
		}
	} else if initialHistory {
		boundary = func(*types.NewsletterMessage) (bool, error) { return false, nil }
	}
	scan, err := scanNewsletterCatchup(
		ctx, api, jid, state.recoveryBefore, count,
		newsletterMaxPollPages, newsletterMaxPollItems,
		time.Now().Add(newsletterMaxPollDuration), boundary,
	)
	if err != nil {
		return state, nil, scan, err
	}
	if scan.pending {
		if err = wa.commitNewsletterCatchupState(ctx, jid, state, state.version, state.watermark, scan.nextBefore, nil); err != nil {
			return state, nil, scan, fmt.Errorf("persist bounded recovery cursor: %w", err)
		}
		return state, scan.messages, scan, nil
	}

	newVersion := state.version
	newWatermark := state.watermark
	if legacyRecovery {
		if scan.boundaryMessage == nil && !scan.historyEnd {
			return state, nil, scan, fmt.Errorf("legacy newsletter watermark %d has no durable receipt in bounded history", state.watermark)
		}
		newVersion = newsletterWatermarkVersion
		if scan.oldest > 0 {
			// Re-anchor below every item inspected in the terminating page. A
			// newer durable row is retained as pending rather than being used to
			// skip an older, non-durable gap on the same page.
			newWatermark = int64(scan.oldest) - 1
		}
	} else if initialHistory {
		if !scan.historyEnd {
			return state, nil, scan, fmt.Errorf("initial newsletter history did not reach a durable boundary")
		}
		newVersion = newsletterWatermarkVersion
		if scan.oldest > 0 {
			newWatermark = int64(scan.oldest) - 1
		}
	}
	var recoveredDurableIDs []int64
	if legacyRecovery {
		recoveredDurableIDs = scan.boundaryIDs
	}
	if err = wa.commitNewsletterCatchupState(ctx, jid, state, newVersion, newWatermark, 0, recoveredDurableIDs); err != nil {
		return state, nil, scan, fmt.Errorf("persist newsletter catch-up state: %w", err)
	}
	return state, scan.messages, scan, nil
}

func (wa *WhatsAppClient) commitNewsletterCatchupState(
	ctx context.Context,
	jid types.JID,
	expected newsletterCatchupState,
	version int,
	watermark int64,
	recoveryBefore types.MessageServerID,
	recoveredDurableIDs []int64,
) error {
	wa.newsletterWatermarkLock.Lock()
	defer wa.newsletterWatermarkLock.Unlock()
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return err
	}
	meta := portal.Metadata.(*waid.PortalMetadata)
	if meta.NewsletterWatermarkVersion != expected.version ||
		meta.LastNewsletterServerID != expected.watermark ||
		meta.NewsletterRecoveryBefore != int64(expected.recoveryBefore) {
		return fmt.Errorf("newsletter catch-up state changed while history was being fetched")
	}
	previousVersion := meta.NewsletterWatermarkVersion
	previousWatermark := meta.LastNewsletterServerID
	previousBefore := meta.NewsletterRecoveryBefore
	previousPending := slices.Clone(meta.NewsletterPendingServerIDs)
	meta.NewsletterWatermarkVersion = version
	meta.LastNewsletterServerID = watermark
	meta.NewsletterRecoveryBefore = int64(recoveryBefore)
	for _, serverID := range recoveredDurableIDs {
		if !slices.Contains(meta.NewsletterPendingServerIDs, serverID) {
			meta.NewsletterPendingServerIDs = append(meta.NewsletterPendingServerIDs, serverID)
		}
	}
	if meta.NewsletterWatermarkVersion >= newsletterWatermarkVersion {
		advanceNewsletterPendingReceipts(meta)
	}
	if len(meta.NewsletterPendingServerIDs) > newsletterMaxPending {
		// Keep the receipts nearest the frontier. Higher durable IDs are safe to
		// forget here: after the continuation resets, bounded history refetches
		// them and bridge message identity makes that replay idempotent.
		meta.NewsletterPendingServerIDs = meta.NewsletterPendingServerIDs[:newsletterMaxPending]
	}
	if err = portal.Save(ctx); err != nil {
		meta.NewsletterWatermarkVersion = previousVersion
		meta.LastNewsletterServerID = previousWatermark
		meta.NewsletterRecoveryBefore = previousBefore
		meta.NewsletterPendingServerIDs = previousPending
		return err
	}
	return nil
}

func (wa *WhatsAppClient) isNewsletterMessageDurable(ctx context.Context, jid types.JID, message *types.NewsletterMessage) (bool, error) {
	messageID := waid.MakeMessageID(jid, jid, message.MessageID)
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return false, err
	}
	parts, err := wa.Main.Bridge.DB.Message.GetAllPartsByID(ctx, portal.Receiver, messageID)
	if err != nil {
		return false, err
	}
	for _, part := range parts {
		if !part.HasFakeMXID() {
			return true, nil
		}
	}
	return false, nil
}

func advanceNewsletterPendingReceipts(meta *waid.PortalMetadata) {
	slices.Sort(meta.NewsletterPendingServerIDs)
	meta.NewsletterPendingServerIDs = slices.Compact(meta.NewsletterPendingServerIDs)
	meta.NewsletterPendingServerIDs = slices.DeleteFunc(meta.NewsletterPendingServerIDs, func(serverID int64) bool {
		return serverID <= meta.LastNewsletterServerID
	})
	for len(meta.NewsletterPendingServerIDs) > 0 && meta.NewsletterPendingServerIDs[0] == meta.LastNewsletterServerID+1 {
		meta.LastNewsletterServerID++
		meta.NewsletterPendingServerIDs = meta.NewsletterPendingServerIDs[1:]
	}
}

func (wa *WhatsAppClient) recordNewsletterDurableReceipt(
	ctx context.Context,
	portal *bridgev2.Portal,
	jid types.JID,
	serverID types.MessageServerID,
) (advanced bool, pending int, err error) {
	wa.newsletterWatermarkLock.Lock()
	defer wa.newsletterWatermarkLock.Unlock()
	meta := portal.Metadata.(*waid.PortalMetadata)
	previousWatermark := meta.LastNewsletterServerID
	previousPending := slices.Clone(meta.NewsletterPendingServerIDs)
	next := int64(serverID)
	if (meta.NewsletterWatermarkVersion >= newsletterWatermarkVersion && next <= meta.LastNewsletterServerID) ||
		slices.Contains(meta.NewsletterPendingServerIDs, next) {
		return false, len(meta.NewsletterPendingServerIDs), nil
	}
	gapClosing := meta.NewsletterWatermarkVersion >= newsletterWatermarkVersion && next == meta.LastNewsletterServerID+1
	if len(meta.NewsletterPendingServerIDs) >= newsletterMaxPending && !gapClosing {
		return false, len(meta.NewsletterPendingServerIDs), fmt.Errorf("newsletter pending receipt ledger reached bounded %d-item cap", newsletterMaxPending)
	}
	meta.NewsletterPendingServerIDs = append(meta.NewsletterPendingServerIDs, next)
	if meta.NewsletterWatermarkVersion >= newsletterWatermarkVersion {
		advanceNewsletterPendingReceipts(meta)
	}
	if err = portal.Save(ctx); err != nil {
		meta.LastNewsletterServerID = previousWatermark
		meta.NewsletterPendingServerIDs = previousPending
		return false, len(previousPending), err
	}
	wa.UserLogin.Log.Debug().Stringer("newsletter_jid", jid).
		Int64("previous_server_id", previousWatermark).
		Int64("server_id", meta.LastNewsletterServerID).
		Int("pending_receipts", len(meta.NewsletterPendingServerIDs)).
		Msg("Recorded durable newsletter receipt")
	return meta.LastNewsletterServerID > previousWatermark, len(meta.NewsletterPendingServerIDs), nil
}

func (evt *WAMessageEvent) recordNewsletterDurableReceipt(ctx context.Context, portal *bridgev2.Portal) {
	parts, err := evt.wa.Main.Bridge.DB.Message.GetAllPartsByID(ctx, portal.Receiver, evt.GetID())
	durableParts := 0
	if err == nil {
		for _, part := range parts {
			if !part.HasFakeMXID() {
				durableParts++
			}
		}
	}
	durable := err == nil && durableParts > 0 && (evt.newsletterExpectedParts == 0 || durableParts >= evt.newsletterExpectedParts)
	advanced := false
	pending := 0
	if durable {
		advanced, pending, err = evt.wa.recordNewsletterDurableReceipt(ctx, portal, evt.Info.Chat, evt.newsletterServerID)
	}
	log := evt.wa.UserLogin.Log.Warn().
		Str("delivery_source", evt.newsletterDeliverySource).
		Stringer("newsletter_jid", evt.Info.Chat).
		Str("message_id", string(evt.Info.ID)).
		Int("server_id", evt.Info.ServerID).
		Int("durable_parts", durableParts).
		Int("expected_parts", evt.newsletterExpectedParts).
		Bool("durably_accepted", durable).
		Bool("watermark_advanced", advanced).
		Int("pending_receipts", pending)
	if err != nil {
		log = log.Err(err)
	}
	log.Msg("Newsletter reliability durable receipt audit")
}

func (wa *WhatsAppClient) newsletterLiveUpdatesLoop(ctx context.Context, api newsletterAPI, jid types.JID) {
	retryDelay := newsletterMinRetryDelay
	for {
		duration, err := api.NewsletterSubscribeLiveUpdates(ctx, jid)
		if ctx.Err() != nil {
			return
		}
		var next time.Duration
		if err != nil {
			wa.UserLogin.Log.Error().Err(err).Stringer("newsletter_jid", jid).
				Bool("rate_limit_shaped", isRateLimitShaped(err)).
				Dur("retry_in", retryDelay).
				Msg("Newsletter metadata live-updates subscription failed; backing off")
			next = jitterNewsletterDelay(retryDelay)
			retryDelay = min(retryDelay*2, newsletterMaxRetryDelay)
		} else {
			renewAfter := duration * 4 / 5
			if renewAfter < time.Minute {
				renewAfter = time.Minute
			}
			next = jitterNewsletterDelay(renewAfter)
			retryDelay = newsletterMinRetryDelay
			wa.UserLogin.Log.Info().Stringer("newsletter_jid", jid).
				Dur("granted_duration", duration).
				Dur("renew_in", next).
				Msg("Subscribed to newsletter reaction/view-count live updates")
		}
		if !waitNewsletterDelay(ctx, next) {
			return
		}
	}
}
