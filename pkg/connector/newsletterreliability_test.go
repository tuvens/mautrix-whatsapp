package connector

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

type newsletterTestAPI struct {
	requests []whatsmeow.GetNewsletterMessagesParams
	pages    map[types.MessageServerID][]*types.NewsletterMessage
	errors   map[types.MessageServerID]error
}

func (api *newsletterTestAPI) GetSubscribedNewsletters(context.Context) ([]*types.NewsletterMetadata, error) {
	return nil, nil
}

func (api *newsletterTestAPI) GetNewsletterMessages(_ context.Context, _ types.JID, params *whatsmeow.GetNewsletterMessagesParams) ([]*types.NewsletterMessage, error) {
	api.requests = append(api.requests, *params)
	if err := api.errors[params.Before]; err != nil {
		return nil, err
	}
	return api.pages[params.Before], nil
}

func (api *newsletterTestAPI) NewsletterSubscribeLiveUpdates(context.Context, types.JID) (time.Duration, error) {
	return 0, nil
}

func testNewsletterMessage(serverID types.MessageServerID) *types.NewsletterMessage {
	return &types.NewsletterMessage{
		MessageServerID: serverID,
		MessageID:       types.MessageID(fmt.Sprintf("message-%d", serverID)),
		Timestamp:       time.Unix(int64(serverID), 0),
		Message:         &waE2E.Message{},
	}
}

func TestNewsletterReliabilityExampleConfigDefaults(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(ExampleConfig), &cfg); err != nil {
		t.Fatalf("failed to parse example config: %v", err)
	}
	got := cfg.NewsletterReliability
	if !got.EnableBackfillPoll || got.PollInterval != 6*time.Hour || got.PollCount != 10 {
		t.Fatalf("unexpected backfill defaults: %+v", got)
	}
	if got.EnableLiveUpdates || len(got.LiveUpdatesChannels) != 0 {
		t.Fatalf("live updates must default off with an empty allowlist: %+v", got)
	}
}

func TestNewsletterDeliverySourceMarksOnlySyntheticPollContext(t *testing.T) {
	if got := newsletterDeliverySource(context.Background()); got != "push" {
		t.Fatalf("plain live context source = %q, want push", got)
	}
	pollCtx := context.WithValue(context.Background(), newsletterPollDeliveryContextKey{}, true)
	if got := newsletterDeliverySource(pollCtx); got != "poll" {
		t.Fatalf("synthetic catch-up context source = %q, want poll", got)
	}
}

func TestCollectNewsletterCatchupPagesToWatermarkAndSortsOldestFirst(t *testing.T) {
	api := &newsletterTestAPI{pages: map[types.MessageServerID][]*types.NewsletterMessage{
		0:   {testNewsletterMessage(105), testNewsletterMessage(104)},
		104: {testNewsletterMessage(103), testNewsletterMessage(100)},
	}}
	jid := types.NewJID("12345", types.NewsletterServer)
	messages, err := collectNewsletterCatchup(context.Background(), api, jid, 100, 2)
	if err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	if len(api.requests) != 2 || api.requests[1].Before != 104 {
		t.Fatalf("unexpected pagination requests: %+v", api.requests)
	}
	want := []types.MessageServerID{103, 104, 105}
	if len(messages) != len(want) {
		t.Fatalf("got %d messages, want %d", len(messages), len(want))
	}
	for i, message := range messages {
		if message.MessageServerID != want[i] {
			t.Fatalf("message %d has server_id %d, want %d", i, message.MessageServerID, want[i])
		}
	}
}

func TestCollectNewsletterCatchupPagesInitialObservationToHistoryEnd(t *testing.T) {
	api := &newsletterTestAPI{pages: map[types.MessageServerID][]*types.NewsletterMessage{
		0:  {testNewsletterMessage(12), testNewsletterMessage(11)},
		11: {testNewsletterMessage(10)},
	}}
	jid := types.NewJID("12345", types.NewsletterServer)
	messages, err := collectNewsletterCatchup(context.Background(), api, jid, 0, 2)
	if err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	if len(api.requests) != 2 || api.requests[1].Before != 11 {
		t.Fatalf("first observation did not page to history end: %+v", api.requests)
	}
	if len(messages) != 3 || messages[0].MessageServerID != 10 || messages[1].MessageServerID != 11 || messages[2].MessageServerID != 12 {
		t.Fatalf("unexpected initial catch-up messages: %+v", messages)
	}
}

func TestCollectNewsletterCatchupRetainsGapAtPageCap(t *testing.T) {
	pages := make(map[types.MessageServerID][]*types.NewsletterMessage)
	var before types.MessageServerID
	serverID := types.MessageServerID(1000)
	for range newsletterMaxPollPages {
		pages[before] = []*types.NewsletterMessage{
			testNewsletterMessage(serverID),
			testNewsletterMessage(serverID - 1),
		}
		before = serverID - 1
		serverID -= 2
	}
	api := &newsletterTestAPI{pages: pages}
	jid := types.NewJID("12345", types.NewsletterServer)
	scan, err := scanNewsletterCatchup(
		context.Background(), api, jid, 0, 2, newsletterMaxPollPages, newsletterMaxPollItems,
		time.Now().Add(time.Minute), func(message *types.NewsletterMessage) (bool, error) {
			return int64(message.MessageServerID) <= 1, nil
		},
	)
	if err != nil {
		t.Fatalf("bounded scan failed: %v", err)
	}
	if !scan.pending || scan.boundedBy != "pages" || scan.nextBefore == 0 {
		t.Fatalf("bounded scan did not retain page cursor: %+v", scan)
	}
	if len(api.requests) != newsletterMaxPollPages {
		t.Fatalf("made %d requests, want cap %d", len(api.requests), newsletterMaxPollPages)
	}
}

type duplicateTestMatrix struct {
	bridgev2.MatrixConnector
	intent *duplicateTestIntent
}

func (m *duplicateTestMatrix) Init(*bridgev2.Bridge) {}
func (m *duplicateTestMatrix) BotIntent() bridgev2.MatrixAPI {
	return m.intent
}
func (m *duplicateTestMatrix) GhostIntent(networkid.UserID) bridgev2.MatrixAPI {
	return m.intent
}
func (m *duplicateTestMatrix) NewUserIntent(context.Context, id.UserID, string) (bridgev2.MatrixAPI, string, error) {
	return m.intent, "", nil
}

type duplicateTestIntent struct {
	bridgev2.MatrixAPI
}

func (i *duplicateTestIntent) GetMXID() id.UserID { return "@bot:example.com" }
func (i *duplicateTestIntent) EnsureJoined(context.Context, id.RoomID, ...bridgev2.EnsureJoinedParams) error {
	return nil
}
func (i *duplicateTestIntent) EnsureInvited(context.Context, id.RoomID, id.UserID) error {
	return nil
}

type duplicateTestNetworkAPI struct {
	bridgev2.NetworkAPI
}

func newNewsletterReliabilityTestClient(t *testing.T) (*WhatsAppClient, *bridgev2.Portal) {
	t.Helper()
	dbName := regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(t.Name(), "-")
	db, err := dbutil.NewWithDialect("file:"+dbName+"?mode=memory&cache=shared", "sqlite3")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	connector := &WhatsAppConnector{}
	matrix := &duplicateTestMatrix{intent: &duplicateTestIntent{}}
	bridge := bridgev2.NewBridge(networkid.BridgeID(dbName), db, zerolog.Nop(), &bridgeconfig.BridgeConfig{}, matrix, connector, commands.NewProcessor)
	bridge.BackgroundCtx = ctx
	if err = bridge.DB.Upgrade(ctx); err != nil {
		t.Fatalf("upgrade bridge database: %v", err)
	}
	user, err := bridge.GetUserByMXID(ctx, "@newsletter-test:example.com")
	if err != nil {
		t.Fatalf("create test user: %v", err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{ID: networkid.UserLoginID(dbName)}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(_ context.Context, login *bridgev2.UserLogin) error {
			login.Client = &duplicateTestNetworkAPI{}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("create test login: %v", err)
	}
	wa := &WhatsAppClient{Main: connector, UserLogin: login}
	jid := types.NewJID("12345", types.NewsletterServer)
	portal, err := bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		t.Fatalf("create test portal: %v", err)
	}
	portal.MXID = "!newsletter:example.com"
	if err = portal.Save(ctx); err != nil {
		t.Fatalf("save test portal: %v", err)
	}
	return wa, portal
}

func setNewsletterTestState(t *testing.T, portal *bridgev2.Portal, watermark int64, version int) {
	t.Helper()
	meta := portal.Metadata.(*waid.PortalMetadata)
	meta.LastNewsletterServerID = watermark
	meta.NewsletterWatermarkVersion = version
	meta.NewsletterPendingServerIDs = nil
	meta.NewsletterRecoveryBefore = 0
	if err := portal.Save(context.Background()); err != nil {
		t.Fatalf("save test watermark: %v", err)
	}
}

func TestNewsletterQueueAcceptanceDoesNotAdvanceWithoutDurableReceipt(t *testing.T) {
	for _, matrixStatus := range []int{401, 403} {
		t.Run(fmt.Sprintf("matrix_%d", matrixStatus), func(t *testing.T) {
			wa, portal := newNewsletterReliabilityTestClient(t)
			jid := types.NewJID("12345", types.NewsletterServer)
			setNewsletterTestState(t, portal, 399, newsletterWatermarkVersion)
			// A queued event whose Matrix send returned this status has no durable
			// bridge message row when PostHandle runs.
			evt := &WAMessageEvent{
				MessageInfoWrapper: &MessageInfoWrapper{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, Sender: jid}, ID: types.MessageID(fmt.Sprintf("message-400-after-%d", matrixStatus)), ServerID: 400}, wa: wa},
				newsletterServerID: 400,
			}
			evt.PostHandle(context.Background(), portal)
			if got := portal.Metadata.(*waid.PortalMetadata).LastNewsletterServerID; got != 399 {
				t.Fatalf("queue acceptance followed by Matrix %d advanced watermark to %d, want durable watermark 399", matrixStatus, got)
			}
		})
	}
}

func TestNewsletterLaterHistoryRetriesFailedInterval(t *testing.T) {
	wa, portal := newNewsletterReliabilityTestClient(t)
	setNewsletterTestState(t, portal, 399, newsletterWatermarkVersion)
	api := &newsletterTestAPI{pages: map[types.MessageServerID][]*types.NewsletterMessage{
		0: {testNewsletterMessage(407), testNewsletterMessage(406), testNewsletterMessage(405), testNewsletterMessage(404), testNewsletterMessage(403), testNewsletterMessage(402), testNewsletterMessage(401), testNewsletterMessage(400), testNewsletterMessage(399)},
	}}
	_, messages, scan, err := wa.prepareNewsletterCatchup(context.Background(), api, types.NewJID("12345", types.NewsletterServer), 10)
	if err != nil {
		t.Fatalf("collect failed interval: %v", err)
	}
	if scan.pending || len(messages) != 8 {
		t.Fatalf("recovered %d messages, want server IDs 400-407", len(messages))
	}
}

func TestNewsletterDurableReceiptsAdvanceOnlyContiguously(t *testing.T) {
	wa, portal := newNewsletterReliabilityTestClient(t)
	jid := types.NewJID("12345", types.NewsletterServer)
	setNewsletterTestState(t, portal, 399, newsletterWatermarkVersion)
	advanced, pending, err := wa.recordNewsletterDurableReceipt(context.Background(), portal, jid, 401)
	if err != nil {
		t.Fatalf("record out-of-order durable receipt: %v", err)
	}
	if got := portal.Metadata.(*waid.PortalMetadata).LastNewsletterServerID; advanced || pending != 1 || got != 399 {
		t.Fatalf("out-of-order receipt advanced watermark to %d, want 399 pending server ID 400", got)
	}
	advanced, pending, err = wa.recordNewsletterDurableReceipt(context.Background(), portal, jid, 400)
	if err != nil {
		t.Fatalf("record gap-closing durable receipt: %v", err)
	}
	if got := portal.Metadata.(*waid.PortalMetadata).LastNewsletterServerID; !advanced || pending != 0 || got != 401 {
		t.Fatalf("gap-closing receipt produced watermark=%d advanced=%t pending=%d, want 401/true/0", got, advanced, pending)
	}
	advanced, pending, err = wa.recordNewsletterDurableReceipt(context.Background(), portal, jid, 400)
	if err != nil || advanced || pending != 0 {
		t.Fatalf("duplicate durable receipt was not idempotent: advanced=%t pending=%d err=%v", advanced, pending, err)
	}
}

func TestNewsletterCrashReplayRecoversFromDurableBridgeMessage(t *testing.T) {
	wa, portal := newNewsletterReliabilityTestClient(t)
	jid := types.NewJID("12345", types.NewsletterServer)
	setNewsletterTestState(t, portal, 399, newsletterWatermarkVersion)
	message := testNewsletterMessage(400)
	remoteID := waid.MakeMessageID(jid, jid, message.MessageID)
	if err := wa.Main.Bridge.DB.Message.Insert(context.Background(), &database.Message{
		ID: remoteID, MXID: "$durable-400", Room: portal.PortalKey, Timestamp: message.Timestamp, Metadata: &waid.MessageMetadata{},
	}); err != nil {
		t.Fatalf("insert durable bridge message: %v", err)
	}
	evt := &WAMessageEvent{
		MessageInfoWrapper: &MessageInfoWrapper{Info: newsletterMessageEvent(jid, message).Info, wa: wa},
		newsletterServerID: message.MessageServerID,
	}
	evt.PostHandle(context.Background(), portal)
	watermark, err := wa.getNewsletterWatermark(context.Background(), jid)
	if err != nil {
		t.Fatalf("load watermark after crash replay: %v", err)
	}
	if watermark != 400 {
		t.Fatalf("restart recovered watermark %d, want durable bridge receipt 400", watermark)
	}
	evt.PostHandle(context.Background(), portal)
	if got := portal.Metadata.(*waid.PortalMetadata).LastNewsletterServerID; got != 400 {
		t.Fatalf("replayed durable receipt advanced twice to %d", got)
	}
}

func TestNewsletterBoundedGapRemainsPendingForNextPass(t *testing.T) {
	jid := types.NewJID("12345", types.NewsletterServer)
	boundary := func(message *types.NewsletterMessage) (bool, error) { return message.MessageServerID <= 1, nil }
	api := &newsletterTestAPI{pages: map[types.MessageServerID][]*types.NewsletterMessage{
		0:   {testNewsletterMessage(1000), testNewsletterMessage(999)},
		999: {testNewsletterMessage(998), testNewsletterMessage(997)},
		997: {testNewsletterMessage(996), testNewsletterMessage(1)},
	}}
	scan, err := scanNewsletterCatchup(context.Background(), api, jid, 0, 2, 2, 100, time.Now().Add(time.Minute), boundary)
	if err != nil || !scan.pending || scan.boundedBy != "pages" || scan.nextBefore != 997 {
		t.Fatalf("page-bounded scan did not retain pending cursor: scan=%+v err=%v", scan, err)
	}
	continued, err := scanNewsletterCatchup(context.Background(), api, jid, scan.nextBefore, 2, 2, 100, time.Now().Add(time.Minute), boundary)
	if err != nil || continued.pending || continued.boundaryMessage == nil || continued.boundaryMessage.MessageServerID != 1 {
		t.Fatalf("continued scan did not close pending gap: scan=%+v err=%v", continued, err)
	}
	itemBounded, err := scanNewsletterCatchup(context.Background(), api, jid, 0, 2, 100, 2, time.Now().Add(time.Minute), boundary)
	if err != nil || !itemBounded.pending || itemBounded.boundedBy != "items" {
		t.Fatalf("item-bounded scan result=%+v err=%v", itemBounded, err)
	}
	timeBounded, err := scanNewsletterCatchup(context.Background(), api, jid, 0, 2, 100, 100, time.Now().Add(-time.Second), boundary)
	if err != nil || !timeBounded.pending || timeBounded.boundedBy != "time" {
		t.Fatalf("time-bounded scan result=%+v err=%v", timeBounded, err)
	}
	for name, apiErr := range map[string]error{"rate_limit": fmt.Errorf("HTTP 429 rate limit"), "history_unavailable": fmt.Errorf("history unavailable")} {
		t.Run(name, func(t *testing.T) {
			failedAPI := &newsletterTestAPI{errors: map[types.MessageServerID]error{0: apiErr}}
			_, gotErr := scanNewsletterCatchup(context.Background(), failedAPI, jid, 0, 2, 2, 100, time.Now().Add(time.Minute), boundary)
			if gotErr == nil || !strings.Contains(gotErr.Error(), apiErr.Error()) {
				t.Fatalf("scan error=%v, want %v", gotErr, apiErr)
			}
		})
	}
}

func TestNewsletterBoundedRecoveryCursorPersistsAcrossPasses(t *testing.T) {
	wa, portal := newNewsletterReliabilityTestClient(t)
	jid := types.NewJID("12345", types.NewsletterServer)
	setNewsletterTestState(t, portal, 1, newsletterWatermarkVersion)
	pages := make(map[types.MessageServerID][]*types.NewsletterMessage)
	var before types.MessageServerID
	serverID := types.MessageServerID(2000)
	for range newsletterMaxPollItems / 100 {
		page := make([]*types.NewsletterMessage, 0, 100)
		for range 100 {
			page = append(page, testNewsletterMessage(serverID))
			serverID--
		}
		pages[before] = page
		before = page[len(page)-1].MessageServerID
	}
	pages[before] = []*types.NewsletterMessage{testNewsletterMessage(serverID), testNewsletterMessage(1)}
	api := &newsletterTestAPI{pages: pages}
	_, messages, scan, err := wa.prepareNewsletterCatchup(context.Background(), api, jid, 100)
	if err != nil || len(messages) != 0 || !scan.pending || scan.boundedBy != "items" {
		t.Fatalf("first bounded pass messages=%d scan=%+v err=%v", len(messages), scan, err)
	}
	meta := portal.Metadata.(*waid.PortalMetadata)
	if meta.NewsletterRecoveryBefore != int64(before) {
		t.Fatalf("persisted recovery cursor=%d, want %d", meta.NewsletterRecoveryBefore, before)
	}
	_, messages, scan, err = wa.prepareNewsletterCatchup(context.Background(), api, jid, 100)
	if err != nil || scan.pending || len(messages) != 1 || messages[0].MessageServerID != serverID {
		t.Fatalf("continued pass messages=%v scan=%+v err=%v", messages, scan, err)
	}
	if meta.NewsletterRecoveryBefore != 0 {
		t.Fatalf("completed recovery left cursor %d, want 0", meta.NewsletterRecoveryBefore)
	}
}

func TestNewsletterObservedStateRecovers400Through407WithoutDuplicating399(t *testing.T) {
	wa, portal := newNewsletterReliabilityTestClient(t)
	jid := types.NewJID("12345", types.NewsletterServer)
	setNewsletterTestState(t, portal, 407, 0)
	durable := testNewsletterMessage(399)
	if err := wa.Main.Bridge.DB.Message.Insert(context.Background(), &database.Message{
		ID: waid.MakeMessageID(jid, jid, durable.MessageID), MXID: "$durable-399", Room: portal.PortalKey, Timestamp: durable.Timestamp, Metadata: &waid.MessageMetadata{},
	}); err != nil {
		t.Fatalf("insert observed durable message: %v", err)
	}
	api := &newsletterTestAPI{pages: map[types.MessageServerID][]*types.NewsletterMessage{
		0: {testNewsletterMessage(407), testNewsletterMessage(406), testNewsletterMessage(405), testNewsletterMessage(404), testNewsletterMessage(403), testNewsletterMessage(402), testNewsletterMessage(401), testNewsletterMessage(400), durable},
	}}
	_, messages, scan, err := wa.prepareNewsletterCatchup(context.Background(), api, jid, 10)
	if err != nil {
		t.Fatalf("recover observed state: %v", err)
	}
	if scan.pending || len(messages) != 8 {
		t.Fatalf("observed-state recovery returned %d messages, want exactly 400-407 and no 399", len(messages))
	}
	for i, message := range messages {
		if want := types.MessageServerID(400 + i); message.MessageServerID != want {
			t.Fatalf("recovered message %d has server ID %d, want %d", i, message.MessageServerID, want)
		}
	}
	meta := portal.Metadata.(*waid.PortalMetadata)
	if meta.NewsletterWatermarkVersion != newsletterWatermarkVersion || meta.LastNewsletterServerID != 399 {
		t.Fatalf("legacy recovery state version=%d watermark=%d, want %d/399", meta.NewsletterWatermarkVersion, meta.LastNewsletterServerID, newsletterWatermarkVersion)
	}
}

func TestBridgeV2DropsFetchedCopyOfPushedNewsletterMessage(t *testing.T) {
	ctx := context.Background()
	db, err := dbutil.NewWithDialect("file:newsletter-dedup?mode=memory&cache=shared", "sqlite3")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	connector := &WhatsAppConnector{}
	matrix := &duplicateTestMatrix{intent: &duplicateTestIntent{}}
	bridge := bridgev2.NewBridge("newsletter-dedup", db, zerolog.Nop(), &bridgeconfig.BridgeConfig{}, matrix, connector, commands.NewProcessor)
	bridge.BackgroundCtx = ctx
	if err = bridge.DB.Upgrade(ctx); err != nil {
		t.Fatalf("upgrade bridge database: %v", err)
	}
	user, err := bridge.GetUserByMXID(ctx, "@newsletter-test:example.com")
	if err != nil {
		t.Fatalf("create test user: %v", err)
	}
	login, err := user.NewLogin(ctx, &database.UserLogin{ID: "newsletter-test"}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(_ context.Context, login *bridgev2.UserLogin) error {
			login.Client = &duplicateTestNetworkAPI{}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("create test login: %v", err)
	}

	wa := &WhatsAppClient{Main: connector, UserLogin: login}
	jid := types.NewJID("12345", types.NewsletterServer)
	portal, err := bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(jid))
	if err != nil {
		t.Fatalf("create test portal: %v", err)
	}
	portal.MXID = "!newsletter:example.com"
	if err = portal.Save(ctx); err != nil {
		t.Fatalf("save test portal: %v", err)
	}

	pushed := &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: jid, Sender: jid},
		ID:            "same-whatsapp-message-id",
		ServerID:      42,
		Timestamp:     time.Unix(42, 0),
	}, Message: &waE2E.Message{}}
	pushedID := waid.MakeMessageID(pushed.Info.Chat, pushed.Info.Sender, pushed.Info.ID)
	if err = bridge.DB.Message.Insert(ctx, &database.Message{
		ID:        pushedID,
		MXID:      "$already-bridged",
		Room:      portal.PortalKey,
		Timestamp: pushed.Info.Timestamp,
		Metadata:  &waid.MessageMetadata{},
	}); err != nil {
		t.Fatalf("insert pushed message: %v", err)
	}

	fetched := newsletterMessageEvent(jid, &types.NewsletterMessage{
		MessageServerID: pushed.Info.ServerID,
		MessageID:       pushed.Info.ID,
		Timestamp:       pushed.Info.Timestamp,
		Message:         pushed.Message,
	})
	remote := &WAMessageEvent{
		MessageInfoWrapper: &MessageInfoWrapper{Info: fetched.Info, wa: wa},
		Message:            fetched.Message,
		MsgEvent:           fetched,
		parsedMessageType:  getMessageType(fetched.Message),
	}
	if remote.GetID() != pushedID {
		t.Fatalf("fetched ID %q differs from pushed ID %q", remote.GetID(), pushedID)
	}

	oldBuffer := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 0
	defer func() { bridgev2.PortalEventBuffer = oldBuffer }()
	result := login.QueueRemoteEvent(remote)
	if !result.Success || result.Queued {
		t.Fatalf("bridgev2 did not finish duplicate handling synchronously: %+v", result)
	}
	parts, err := bridge.DB.Message.GetAllPartsByID(ctx, portal.Receiver, networkid.MessageID(pushedID))
	if err != nil {
		t.Fatalf("reload message: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("duplicate handling changed message part count to %d, want 1", len(parts))
	}
}
