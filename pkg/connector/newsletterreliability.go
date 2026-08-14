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
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

const (
	newsletterInitialPollDelay = 2 * time.Minute
	newsletterMaxPollPages     = 100
	newsletterMinRetryDelay    = time.Minute
	newsletterMaxRetryDelay    = time.Hour
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
	if !waitNewsletterDelay(ctx, jitterNewsletterDelay(newsletterInitialPollDelay)) {
		return
	}
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
	watermark, err := wa.getNewsletterWatermark(ctx, jid)
	if err != nil {
		return fmt.Errorf("load watermark: %w", err)
	}
	messages, err := collectNewsletterCatchup(ctx, api, jid, watermark, cfg.PollCount)
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
		Int64("starting_watermark", watermark).
		Int("candidate_count", len(messages)).
		Msg("Newsletter reliability poll audit")
	return nil
}

func collectNewsletterCatchup(ctx context.Context, api newsletterAPI, jid types.JID, watermark int64, count int) ([]*types.NewsletterMessage, error) {
	var collected []*types.NewsletterMessage
	var before types.MessageServerID
	for page := 0; page < newsletterMaxPollPages; page++ {
		messages, err := api.GetNewsletterMessages(ctx, jid, &whatsmeow.GetNewsletterMessagesParams{
			Count:  count,
			Before: before,
		})
		if err != nil {
			return nil, err
		}
		if len(messages) == 0 {
			break
		}
		reachedWatermark := false
		oldest := messages[0].MessageServerID
		for _, message := range messages {
			if message.MessageServerID < oldest {
				oldest = message.MessageServerID
			}
			if int64(message.MessageServerID) <= watermark {
				reachedWatermark = true
				continue
			}
			collected = append(collected, message)
		}
		if watermark == 0 || reachedWatermark || len(messages) < count {
			break
		}
		if oldest == 0 || oldest == before {
			return nil, fmt.Errorf("newsletter pagination did not advance before=%d", oldest)
		}
		before = oldest
		if page == newsletterMaxPollPages-1 {
			return nil, fmt.Errorf("newsletter gap exceeded bounded %d-page catch-up", newsletterMaxPollPages)
		}
	}
	slices.SortFunc(collected, func(a, b *types.NewsletterMessage) int {
		return cmp.Compare(a.MessageServerID, b.MessageServerID)
	})
	return collected, nil
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

func (wa *WhatsAppClient) advanceNewsletterWatermark(ctx context.Context, jid types.JID, serverID types.MessageServerID) (bool, error) {
	wa.newsletterWatermarkLock.Lock()
	defer wa.newsletterWatermarkLock.Unlock()
	portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		return false, err
	}
	meta := portal.Metadata.(*waid.PortalMetadata)
	next := int64(serverID)
	if next <= meta.LastNewsletterServerID {
		return false, nil
	}
	previous := meta.LastNewsletterServerID
	meta.LastNewsletterServerID = next
	if err = portal.Save(ctx); err != nil {
		meta.LastNewsletterServerID = previous
		return false, err
	}
	wa.UserLogin.Log.Debug().Stringer("newsletter_jid", jid).
		Int64("previous_server_id", previous).
		Int64("server_id", next).
		Msg("Advanced newsletter reliability watermark")
	return true, nil
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
