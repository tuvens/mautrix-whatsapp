package connector

import (
	"context"
	"fmt"
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
}

func (api *newsletterTestAPI) GetSubscribedNewsletters(context.Context) ([]*types.NewsletterMetadata, error) {
	return nil, nil
}

func (api *newsletterTestAPI) GetNewsletterMessages(_ context.Context, _ types.JID, params *whatsmeow.GetNewsletterMessagesParams) ([]*types.NewsletterMessage, error) {
	api.requests = append(api.requests, *params)
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

func TestCollectNewsletterCatchupRefusesGapBeyondPageCap(t *testing.T) {
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
	_, err := collectNewsletterCatchup(context.Background(), api, jid, 1, 2)
	if err == nil || !strings.Contains(err.Error(), "exceeded bounded") {
		t.Fatalf("expected bounded-gap refusal, got %v", err)
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
