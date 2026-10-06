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
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
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
	newsletterUndecodableLimit = 3
	newsletterQuarantineDir    = "undecodable-newsletters"
)

type newsletterAPI interface {
	GetSubscribedNewsletters(context.Context) ([]*types.NewsletterMetadata, error)
	GetNewsletterMessages(context.Context, types.JID, *whatsmeow.GetNewsletterMessagesParams) ([]*newsletterFetchedMessage, error)
	NewsletterSubscribeLiveUpdates(context.Context, types.JID) (time.Duration, error)
}

type newsletterFetchedMessage struct {
	*types.NewsletterMessage
	bodyAbsent  bool
	rawEnvelope []byte
}

type whatsmeowNewsletterAPI struct {
	*whatsmeow.Client
}

func (api *whatsmeowNewsletterAPI) GetNewsletterMessages(
	ctx context.Context,
	jid types.JID,
	params *whatsmeow.GetNewsletterMessagesParams,
) ([]*newsletterFetchedMessage, error) {
	attrs := waBinary.Attrs{
		"type": "jid",
		"jid":  jid,
	}
	if params != nil {
		if params.Count != 0 {
			attrs["count"] = params.Count
		}
		if params.Before != 0 {
			attrs["before"] = params.Before
		}
	}
	resp, err := api.DangerousInternals().SendIQ(ctx, whatsmeow.DangerousInfoQuery{
		Namespace: "newsletter",
		Type:      whatsmeow.DangerousInfoQueryType("get"),
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag:   "messages",
			Attrs: attrs,
		}},
	})
	if err != nil {
		return nil, err
	}
	messagesNode, ok := resp.GetOptionalChildByTag("messages")
	if !ok {
		return nil, fmt.Errorf("newsletter messages response is missing messages element")
	}
	rawMessages := messagesNode.GetChildrenByTag("message")
	parsedMessages := api.DangerousInternals().ParseNewsletterMessages(&messagesNode)
	if len(rawMessages) != len(parsedMessages) {
		return nil, fmt.Errorf("newsletter message parser returned %d messages for %d raw envelopes", len(parsedMessages), len(rawMessages))
	}
	messages := make([]*newsletterFetchedMessage, len(parsedMessages))
	for i, message := range parsedMessages {
		if message == nil {
			return nil, fmt.Errorf("newsletter message parser returned a nil message at envelope %d", i)
		}
		_, hasPlaintext := rawMessages[i].GetOptionalChildByTag("plaintext")
		rawEnvelope, marshalErr := waBinary.Marshal(rawMessages[i])
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal raw newsletter envelope %d: %w", i, marshalErr)
		}
		messages[i] = &newsletterFetchedMessage{
			NewsletterMessage: message,
			bodyAbsent:        !hasPlaintext,
			rawEnvelope:       rawEnvelope,
		}
	}
	return messages, nil
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
	go wa.newsletterReliabilityLoop(ctx, &whatsmeowNewsletterAPI{Client: wa.Client})
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
		if message.bodyAbsent {
			if err = wa.recordNewsletterNothingToBridgeReceipt(ctx, jid, message.NewsletterMessage); err != nil {
				return fmt.Errorf("record nothing-to-bridge receipt for server_id %d: %w", message.MessageServerID, err)
			}
			continue
		}
		if message.Message == nil {
			quarantined, quarantineErr := wa.recordNewsletterUndecodableFailure(ctx, jid, message)
			if quarantineErr != nil {
				return fmt.Errorf("record undecodable newsletter server_id %d: %w", message.MessageServerID, quarantineErr)
			}
			if quarantined {
				continue
			}
			return fmt.Errorf("newsletter server_id %d contained plaintext that could not be decoded", message.MessageServerID)
		}
		if err = wa.clearNewsletterUndecodableFailure(ctx, jid, message.MessageServerID); err != nil {
			return fmt.Errorf("clear undecodable failure count for server_id %d: %w", message.MessageServerID, err)
		}
		evt := newsletterMessageEvent(jid, message.NewsletterMessage)
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
	messages        []*newsletterFetchedMessage
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
type newsletterCatchupBoundaryStop func(*types.NewsletterMessage, bool) bool

func collectNewsletterCatchup(ctx context.Context, api newsletterAPI, jid types.JID, watermark int64, count int) ([]*newsletterFetchedMessage, error) {
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
	return scanNewsletterCatchupWithStop(ctx, api, jid, before, count, maxPages, maxItems, deadline, boundary, nil)
}

func scanNewsletterCatchupWithStop(
	ctx context.Context,
	api newsletterAPI,
	jid types.JID,
	before types.MessageServerID,
	count int,
	maxPages int,
	maxItems int,
	deadline time.Time,
	boundary newsletterCatchupBoundary,
	stopAtBoundary newsletterCatchupBoundaryStop,
) (newsletterCatchupScan, error) {
	result := newsletterCatchupScan{nextBefore: before}
	if count <= 0 {
		return result, fmt.Errorf("newsletter history count must be positive")
	}
	if stopAtBoundary == nil {
		stopAtBoundary = func(_ *types.NewsletterMessage, atBoundary bool) bool { return atBoundary }
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
			if message == nil || message.NewsletterMessage == nil || message.MessageServerID <= 0 {
				continue
			}
			if oldest == 0 || message.MessageServerID < oldest {
				oldest = message.MessageServerID
			}
			atBoundary, err := boundary(message.NewsletterMessage)
			if err != nil {
				return result, err
			}
			if atBoundary {
				result.boundaryIDs = append(result.boundaryIDs, int64(message.MessageServerID))
				if stopAtBoundary(message.NewsletterMessage, true) {
					if result.boundaryMessage == nil || message.MessageServerID < result.boundaryMessage.MessageServerID {
						result.boundaryMessage = message.NewsletterMessage
					}
					reachedBoundary = true
				}
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
	slices.SortFunc(result.messages, func(a, b *newsletterFetchedMessage) int {
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
	wa.Main.newsletterWatermarkLock.Lock()
	defer wa.Main.newsletterWatermarkLock.Unlock()
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return 0, err
	}
	return portal.Metadata.(*waid.PortalMetadata).LastNewsletterServerID, nil
}

func (wa *WhatsAppClient) getNewsletterCatchupState(ctx context.Context, jid types.JID) (newsletterCatchupState, error) {
	wa.Main.newsletterWatermarkLock.Lock()
	defer wa.Main.newsletterWatermarkLock.Unlock()
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
) (newsletterCatchupState, []*newsletterFetchedMessage, newsletterCatchupScan, error) {
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
	stopAtBoundary := newsletterCatchupBoundaryStop(nil)
	if legacyRecovery {
		// A durable post newer than a legacy queue-accepted watermark is not a
		// safe migration anchor: the missing interval may be on an older page.
		// Keep scanning until durability is proven at or below the legacy
		// watermark, or retain the bounded continuation for the next pass.
		stopAtBoundary = func(message *types.NewsletterMessage, durable bool) bool {
			return durable && int64(message.MessageServerID) <= state.watermark
		}
	}
	scan, err := scanNewsletterCatchupWithStop(
		ctx, api, jid, state.recoveryBefore, count,
		newsletterMaxPollPages, newsletterMaxPollItems,
		time.Now().Add(newsletterMaxPollDuration), boundary, stopAtBoundary,
	)
	if err != nil {
		return state, nil, scan, err
	}
	if legacyRecovery && scan.boundaryMessage == nil {
		if !scan.pending {
			scan.pending = true
			if scan.historyEnd && scan.items == 0 {
				scan.boundedBy = "history_empty"
			} else {
				scan.boundedBy = "anchor_not_found"
			}
		}
		wa.UserLogin.Log.Warn().
			Stringer("newsletter_jid", jid).
			Int64("legacy_watermark", state.watermark).
			Str("pending_reason", scan.boundedBy).
			Int("history_items", scan.items).
			Msg("Legacy newsletter recovery has no proven durable anchor; keeping migration pending")
	}
	if scan.pending {
		var recoveredDurableIDs []int64
		if legacyRecovery {
			recoveredDurableIDs = scan.boundaryIDs
		}
		if err = wa.commitNewsletterCatchupState(ctx, jid, state, state.version, state.watermark, scan.nextBefore, recoveredDurableIDs); err != nil {
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
	wa.Main.newsletterWatermarkLock.Lock()
	defer wa.Main.newsletterWatermarkLock.Unlock()
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
	return wa.recordNewsletterTerminalReceipt(ctx, portal, jid, serverID, "durable_delivery")
}

func (wa *WhatsAppClient) recordNewsletterNothingToBridgeReceipt(
	ctx context.Context,
	jid types.JID,
	message *types.NewsletterMessage,
) error {
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return err
	}
	advanced, pending, err := wa.recordNewsletterTerminalReceipt(ctx, portal, jid, message.MessageServerID, "nothing_to_bridge")
	log := wa.UserLogin.Log.Warn().
		Stringer("newsletter_jid", jid).
		Str("message_id", string(message.MessageID)).
		Int("server_id", message.MessageServerID).
		Bool("terminal_receipt", err == nil).
		Bool("durably_delivered", false).
		Bool("watermark_advanced", advanced).
		Int("pending_receipts", pending).
		Str("reason", "no_message_body")
	if err != nil {
		log = log.Err(err)
	}
	log.Msg("Newsletter reliability nothing-to-bridge receipt audit")
	return err
}

func newsletterQuarantineFilename(jid types.JID, serverID types.MessageServerID) string {
	safeJID := strings.Map(func(char rune) rune {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9':
			return char
		case char == '@', char == '.', char == '-', char == '_':
			return char
		default:
			return '_'
		}
	}, jid.String())
	return fmt.Sprintf("server-%d_%s.bin", serverID, safeJID)
}

func syncNewsletterDirectory(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (wa *WhatsAppClient) writeNewsletterQuarantineEnvelope(
	jid types.JID,
	message *newsletterFetchedMessage,
) (string, error) {
	if wa.Main.RuntimeDataDir == "" {
		return "", fmt.Errorf("bridge runtime data directory is not configured")
	}
	if len(message.rawEnvelope) == 0 {
		return "", fmt.Errorf("raw envelope is empty")
	}
	runtimeRoot, err := os.OpenRoot(wa.Main.RuntimeDataDir)
	if err != nil {
		return "", fmt.Errorf("open bridge runtime data directory: %w", err)
	}
	defer runtimeRoot.Close()
	if err = runtimeRoot.MkdirAll(newsletterQuarantineDir, 0o700); err != nil {
		return "", fmt.Errorf("create quarantine directory: %w", err)
	}
	if err = syncNewsletterDirectory(runtimeRoot); err != nil {
		return "", fmt.Errorf("sync bridge runtime data directory: %w", err)
	}
	quarantineRoot, err := runtimeRoot.OpenRoot(newsletterQuarantineDir)
	if err != nil {
		return "", fmt.Errorf("open quarantine directory: %w", err)
	}
	defer quarantineRoot.Close()
	randomSuffix := make([]byte, 16)
	if _, err = cryptorand.Read(randomSuffix); err != nil {
		return "", fmt.Errorf("generate quarantine temp name: %w", err)
	}
	tempName := fmt.Sprintf(".undecodable-%x.tmp", randomSuffix)
	temp, err := quarantineRoot.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create quarantine temp file: %w", err)
	}
	published := false
	defer func() {
		_ = temp.Close()
		if !published {
			_ = quarantineRoot.Remove(tempName)
		}
	}()
	if _, err = temp.Write(message.rawEnvelope); err != nil {
		return "", fmt.Errorf("write quarantine temp file: %w", err)
	}
	if err = temp.Sync(); err != nil {
		return "", fmt.Errorf("sync quarantine temp file: %w", err)
	}
	if err = temp.Close(); err != nil {
		return "", fmt.Errorf("close quarantine temp file: %w", err)
	}
	destinationName := newsletterQuarantineFilename(jid, message.MessageServerID)
	if err = quarantineRoot.Rename(tempName, destinationName); err != nil {
		return "", fmt.Errorf("publish quarantine file: %w", err)
	}
	published = true
	if err = syncNewsletterDirectory(quarantineRoot); err != nil {
		return "", fmt.Errorf("sync quarantine directory: %w", err)
	}
	return filepath.Join(wa.Main.RuntimeDataDir, newsletterQuarantineDir, destinationName), nil
}

func (wa *WhatsAppClient) recordNewsletterUndecodableFailure(
	ctx context.Context,
	jid types.JID,
	message *newsletterFetchedMessage,
) (bool, error) {
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return false, err
	}
	wa.Main.newsletterWatermarkLock.Lock()
	defer wa.Main.newsletterWatermarkLock.Unlock()
	meta := portal.Metadata.(*waid.PortalMetadata)
	previousFailures := maps.Clone(meta.NewsletterUndecodableFailures)
	if meta.NewsletterUndecodableFailures == nil {
		meta.NewsletterUndecodableFailures = make(map[int64]int)
	}
	serverID := int64(message.MessageServerID)
	failures := meta.NewsletterUndecodableFailures[serverID] + 1
	meta.NewsletterUndecodableFailures[serverID] = failures
	if failures < newsletterUndecodableLimit {
		if err = portal.Save(ctx); err != nil {
			meta.NewsletterUndecodableFailures = previousFailures
			return false, err
		}
		wa.UserLogin.Log.Warn().Stringer("newsletter_jid", jid).
			Int64("server_id", serverID).
			Int("consecutive_decode_failures", failures).
			Int("quarantine_threshold", newsletterUndecodableLimit).
			Msg("Newsletter plaintext remained undecodable; keeping watermark unchanged")
		return false, nil
	}

	previousWatermark := meta.LastNewsletterServerID
	previousPending := slices.Clone(meta.NewsletterPendingServerIDs)
	if (meta.NewsletterWatermarkVersion >= newsletterWatermarkVersion && serverID <= meta.LastNewsletterServerID) ||
		slices.Contains(meta.NewsletterPendingServerIDs, serverID) {
		delete(meta.NewsletterUndecodableFailures, serverID)
		if len(meta.NewsletterUndecodableFailures) == 0 {
			meta.NewsletterUndecodableFailures = nil
		}
		if err = portal.Save(ctx); err != nil {
			meta.NewsletterUndecodableFailures = previousFailures
			return false, err
		}
		return true, nil
	}
	gapClosing := meta.NewsletterWatermarkVersion >= newsletterWatermarkVersion && serverID == meta.LastNewsletterServerID+1
	if len(meta.NewsletterPendingServerIDs) >= newsletterMaxPending && !gapClosing {
		meta.NewsletterUndecodableFailures = previousFailures
		return false, fmt.Errorf("newsletter pending receipt ledger reached bounded %d-item cap", newsletterMaxPending)
	}
	quarantinePath, err := wa.writeNewsletterQuarantineEnvelope(jid, message)
	if err != nil {
		meta.NewsletterUndecodableFailures = previousFailures
		return false, err
	}
	meta.NewsletterPendingServerIDs = append(meta.NewsletterPendingServerIDs, serverID)
	if meta.NewsletterWatermarkVersion >= newsletterWatermarkVersion {
		advanceNewsletterPendingReceipts(meta)
	}
	delete(meta.NewsletterUndecodableFailures, serverID)
	if len(meta.NewsletterUndecodableFailures) == 0 {
		meta.NewsletterUndecodableFailures = nil
	}
	if err = portal.Save(ctx); err != nil {
		meta.LastNewsletterServerID = previousWatermark
		meta.NewsletterPendingServerIDs = previousPending
		meta.NewsletterUndecodableFailures = previousFailures
		return false, err
	}
	wa.UserLogin.Log.Warn().Stringer("newsletter_jid", jid).
		Str("message_id", string(message.MessageID)).
		Int64("server_id", serverID).
		Str("receipt_type", "undecodable_quarantined").
		Int("consecutive_decode_failures", failures).
		Str("quarantine_file", filepath.Base(quarantinePath)).
		Bool("terminal_receipt", true).
		Bool("durably_delivered", false).
		Bool("watermark_advanced", meta.LastNewsletterServerID > previousWatermark).
		Int("pending_receipts", len(meta.NewsletterPendingServerIDs)).
		Msg("Newsletter reliability undecodable quarantine receipt audit")
	return true, nil
}

func (wa *WhatsAppClient) clearNewsletterUndecodableFailure(
	ctx context.Context,
	jid types.JID,
	serverID types.MessageServerID,
) error {
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return err
	}
	wa.Main.newsletterWatermarkLock.Lock()
	defer wa.Main.newsletterWatermarkLock.Unlock()
	meta := portal.Metadata.(*waid.PortalMetadata)
	key := int64(serverID)
	if _, exists := meta.NewsletterUndecodableFailures[key]; !exists {
		return nil
	}
	previousFailures := maps.Clone(meta.NewsletterUndecodableFailures)
	delete(meta.NewsletterUndecodableFailures, key)
	if len(meta.NewsletterUndecodableFailures) == 0 {
		meta.NewsletterUndecodableFailures = nil
	}
	if err = portal.Save(ctx); err != nil {
		meta.NewsletterUndecodableFailures = previousFailures
		return err
	}
	wa.UserLogin.Log.Debug().Stringer("newsletter_jid", jid).
		Int("server_id", serverID).
		Msg("Cleared newsletter undecodable failure count after decode success")
	return nil
}

func (wa *WhatsAppClient) recordNewsletterTerminalReceipt(
	ctx context.Context,
	portal *bridgev2.Portal,
	jid types.JID,
	serverID types.MessageServerID,
	receiptType string,
) (advanced bool, pending int, err error) {
	wa.Main.newsletterWatermarkLock.Lock()
	defer wa.Main.newsletterWatermarkLock.Unlock()
	meta := portal.Metadata.(*waid.PortalMetadata)
	previousWatermark := meta.LastNewsletterServerID
	previousPending := slices.Clone(meta.NewsletterPendingServerIDs)
	previousFailures := maps.Clone(meta.NewsletterUndecodableFailures)
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
	delete(meta.NewsletterUndecodableFailures, next)
	if len(meta.NewsletterUndecodableFailures) == 0 {
		meta.NewsletterUndecodableFailures = nil
	}
	if err = portal.Save(ctx); err != nil {
		meta.LastNewsletterServerID = previousWatermark
		meta.NewsletterPendingServerIDs = previousPending
		meta.NewsletterUndecodableFailures = previousFailures
		return false, len(previousPending), err
	}
	wa.UserLogin.Log.Debug().Stringer("newsletter_jid", jid).
		Str("receipt_type", receiptType).
		Int64("previous_server_id", previousWatermark).
		Int64("server_id", meta.LastNewsletterServerID).
		Int("pending_receipts", len(meta.NewsletterPendingServerIDs)).
		Msg("Recorded terminal newsletter receipt")
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
