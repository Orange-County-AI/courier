package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPort                = 8788
	defaultBind                = "127.0.0.1"
	defaultPromptTimeout       = 120 * time.Second
	defaultTickInterval        = 15 * time.Second
	defaultShadowHeartbeat     = 15 * time.Minute
	defaultRedeliverGrace      = 5 * time.Minute
	defaultRedeliverMaxBackoff = 30 * time.Minute
	defaultRedeliverReadFactor = 4
)

type HerdrOptions struct {
	SocketPath string
	Session    string
	Bin        string
}

type MattermostOptions struct {
	Enabled       bool
	URL           string
	BotToken      string
	AttachmentDir string
	BotUserID     string
}

type GmailOptions struct {
	Enabled       bool
	AccountsJSON  string
	AccountsFile  string
	AttachmentDir string
	PollInterval  time.Duration
}

type KaneoOptions struct {
	Enabled       bool
	ListenPort    int
	WebhookSecret string
	APIBase       string
	BotKey        string
	WorkspaceID   string
	BotActor      string
}
type TelegramOptions struct {
	Enabled             bool
	ListenPort          int
	BotToken            string
	WebhookSecret       string
	BotUsername         string
	BotUserID           string
	AllowedUserIDs      string
	AllowedChatIDs      string
	AttachmentDir       string
	GroupRequireMention bool
	RequireVisibleAck   bool
	ClearDisabled       bool
	ClearAck            string
	DisconnectNotice    string
}

type ServeOptions struct {
	Org                 string
	Target              string
	DataDir             string
	DBPath              string
	Port                int
	Bind                string
	Socket              string
	PromptTimeout       time.Duration
	TickInterval        time.Duration
	RedeliverGrace      time.Duration
	RedeliverMaxBackoff time.Duration
	RedeliverReadFactor float64
	Connectors          []string
	Shadow              ShadowMode
	ShadowHeartbeat     time.Duration
	EnvelopePreview     bool
	DraftGuard          bool
	DraftNotify         bool
	Herdr               HerdrOptions
	Mattermost          MattermostOptions
	Gmail               GmailOptions
	Telegram            TelegramOptions
	Kaneo               KaneoOptions
}

type MCPOptions struct {
	Agent            string
	HostURL          string
	ManifestCacheDir string
}

type envLookup func(string) (string, bool)

// env reads the new name and its cutover-compatible predecessor as one value.
// A silent precedence rule is the confident-wrong-answer shape: when operators
// set both names differently, boot must refuse and name both knobs.
func env(lookup envLookup, suffix string) (string, bool, error) {
	courierName := "COURIER_" + suffix
	channelName := "CHANNEL_" + suffix
	courierValue, courierSet := lookup(courierName)
	channelValue, channelSet := lookup(channelName)
	if courierSet && channelSet && courierValue != channelValue {
		return "", false, fmt.Errorf("%s and %s are both set and differ", courierName, channelName)
	}
	if courierSet {
		return courierValue, true, nil
	}
	return channelValue, channelSet, nil
}

func LoadServeOptions(warn func(string)) (ServeOptions, error) {
	return serveOptionsFromEnv(os.LookupEnv, warn)
}

func serveOptionsFromEnv(lookup envLookup, warn func(string)) (ServeOptions, error) {
	dual := func(name string) (string, bool, error) {
		return env(lookup, name)
	}
	value := func(name string) (string, bool) {
		raw, ok := lookup(name)
		return strings.TrimSpace(raw), ok
	}

	org, _, err := dual("ORG")
	if err != nil {
		return ServeOptions{}, err
	}
	org = strings.TrimSpace(org)
	target, _, err := dual("TARGET")
	if err != nil {
		return ServeOptions{}, err
	}
	target = strings.TrimSpace(target)
	if org == "" || target == "" {
		return ServeOptions{}, fmt.Errorf("COURIER_ORG/CHANNEL_ORG and COURIER_TARGET/CHANNEL_TARGET are required, including in shadow mode")
	}

	dataDir, _, err := dual("DATA_DIR")
	if err != nil {
		return ServeOptions{}, err
	}
	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		dataDir = "./data"
	}
	dbPath, _, err := dual("DB_PATH")
	if err != nil {
		return ServeOptions{}, err
	}
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		dbPath = filepath.Join(dataDir, org+".sqlite")
	}

	port, err := dualInt(dual, "PORT", defaultPort, 1, 65535)
	if err != nil {
		return ServeOptions{}, err
	}
	bind, _, err := dual("BIND")
	if err != nil {
		return ServeOptions{}, err
	}
	bind = strings.TrimSpace(bind)
	if bind == "" {
		bind = defaultBind
	}
	socket, _, err := dual("SOCKET")
	if err != nil {
		return ServeOptions{}, err
	}

	promptTimeout, err := dualMilliseconds(dual, "PROMPT_TIMEOUT_MS", defaultPromptTimeout, false)
	if err != nil {
		return ServeOptions{}, err
	}
	tickInterval, err := dualMilliseconds(dual, "TICK_MS", defaultTickInterval, true)
	if err != nil {
		return ServeOptions{}, err
	}
	shadowHeartbeat, err := dualMilliseconds(dual, "SHADOW_HEARTBEAT_MS", defaultShadowHeartbeat, true)
	if err != nil {
		return ServeOptions{}, err
	}
	redeliverGrace, err := dualPositiveMillisecondsOrDefault(dual, "REDELIVER_GRACE_MS", defaultRedeliverGrace)
	if err != nil {
		return ServeOptions{}, err
	}
	redeliverMaxBackoff, err := dualPositiveMillisecondsOrDefault(dual, "REDELIVER_MAX_BACKOFF_MS", defaultRedeliverMaxBackoff)
	if err != nil {
		return ServeOptions{}, err
	}
	readFactor, err := dualPositiveFloatOrDefault(dual, "REDELIVER_READ_FACTOR", defaultRedeliverReadFactor)
	if err != nil {
		return ServeOptions{}, err
	}

	connectorsRaw, _, err := dual("CONNECTORS")
	if err != nil {
		return ServeOptions{}, err
	}
	connectors := splitList(connectorsRaw)
	shadowRaw, _, err := dual("SHADOW")
	if err != nil {
		return ServeOptions{}, err
	}
	previewRaw, _, err := dual("ENVELOPE_PREVIEW")
	if err != nil {
		return ServeOptions{}, err
	}
	draftGuardRaw, _, err := dual("DRAFT_GUARD")
	if err != nil {
		return ServeOptions{}, err
	}
	draftNotifyRaw, _, err := dual("DRAFT_NOTIFY")
	if err != nil {
		return ServeOptions{}, err
	}
	if _, set := lookup("CHANNEL_ENVELOPE_TODO"); set && warn != nil {
		warn("CHANNEL_ENVELOPE_TODO is set but no longer has any effect; courier/1 has no <todo> element")
	}

	mmURL, _ := value("MATTERMOST_URL")
	mmToken, _ := value("MATTERMOST_BOT_TOKEN")
	mmAttachmentDir, _ := value("MATTERMOST_ATTACHMENT_DIR")
	if mmAttachmentDir == "" {
		mmAttachmentDir = "./data/attachments/mattermost"
	}
	mmBotUserID, _ := value("MATTERMOST_BOT_USER_ID")

	gmailJSON, _ := value("GMAIL_ACCOUNTS_JSON")
	gmailFile, _ := value("GMAIL_ACCOUNTS_FILE")
	gmailAttachmentDir, _ := value("GMAIL_ATTACHMENT_DIR")
	if gmailAttachmentDir == "" {
		gmailAttachmentDir = "./data/attachments/gmail"
	}
	gmailPoll, err := exactSeconds(lookup, "GMAIL_POLL_SECONDS")
	if err != nil {
		return ServeOptions{}, err
	}

	telegramPortRaw, telegramPortSet := value("TELEGRAM_LISTEN_PORT")
	telegram := TelegramOptions{}
	if telegramPortSet && telegramPortRaw != "" {
		telegram.Enabled = true
		telegram.ListenPort, err = parseInt("TELEGRAM_LISTEN_PORT", telegramPortRaw, 1, 65535)
		if err != nil {
			return ServeOptions{}, err
		}
		telegram.BotToken, _ = value("TELEGRAM_BOT_TOKEN")
		telegram.WebhookSecret, _ = value("TELEGRAM_WEBHOOK_SECRET")
		telegram.BotUsername, _ = value("TELEGRAM_BOT_USERNAME")
		telegram.BotUsername = strings.TrimPrefix(telegram.BotUsername, "@")
		telegram.BotUserID, _ = value("TELEGRAM_BOT_USER_ID")
		telegram.AllowedUserIDs, _ = value("TELEGRAM_ALLOWED_USER_IDS")
		telegram.AllowedChatIDs, _ = value("TELEGRAM_ALLOWED_CHAT_IDS")
		telegram.AttachmentDir, _ = value("TELEGRAM_ATTACHMENT_DIR")
		if telegram.AttachmentDir == "" {
			telegram.AttachmentDir = "./data/attachments/telegram"
		}
		groupMention, _ := value("TELEGRAM_GROUP_REQUIRE_MENTION")
		telegram.GroupRequireMention = parseShadow(groupMention)
		requireVisibleAck, _ := value("TELEGRAM_REQUIRE_VISIBLE_ACK")
		telegram.RequireVisibleAck = parseShadow(requireVisibleAck)
		clearDisabled, _ := value("TELEGRAM_CLEAR_DISABLED")
		telegram.ClearDisabled = parseShadow(clearDisabled)
		telegram.ClearAck, _ = value("TELEGRAM_CLEAR_ACK")
		if telegram.ClearAck == "" {
			telegram.ClearAck = "thread cleared. next message starts fresh."
		}
		telegram.DisconnectNotice, _ = value("TELEGRAM_DISCONNECT_NOTICE")
		missing := make([]string, 0, 3)
		if telegram.BotToken == "" {
			missing = append(missing, "TELEGRAM_BOT_TOKEN")
		}
		if telegram.WebhookSecret == "" {
			missing = append(missing, "TELEGRAM_WEBHOOK_SECRET")
		}
		if telegram.AllowedUserIDs == "" && telegram.AllowedChatIDs == "" {
			missing = append(missing, "TELEGRAM_ALLOWED_USER_IDS or TELEGRAM_ALLOWED_CHAT_IDS")
		}
		if len(missing) != 0 {
			return ServeOptions{}, fmt.Errorf("telegram connector is partially configured — missing %s", strings.Join(missing, ", "))
		}
	}

	kaneoPortRaw, kaneoPortSet := value("KANEO_LISTEN_PORT")
	kaneo := KaneoOptions{}
	if kaneoPortSet && kaneoPortRaw != "" {
		kaneo.Enabled = true
		kaneo.ListenPort, err = parseInt("KANEO_LISTEN_PORT", kaneoPortRaw, 1, 65535)
		if err != nil {
			return ServeOptions{}, err
		}
		kaneo.WebhookSecret, _ = value("KANEO_CHANNEL_WEBHOOK_SECRET")
		kaneo.APIBase, _ = value("KANEO_API_BASE")
		kaneo.BotKey, _ = value("KANEO_BOT_KEY")
		missing := make([]string, 0, 3)
		if kaneo.WebhookSecret == "" {
			missing = append(missing, "KANEO_CHANNEL_WEBHOOK_SECRET")
		}
		if kaneo.APIBase == "" {
			missing = append(missing, "KANEO_API_BASE")
		}
		if kaneo.BotKey == "" {
			missing = append(missing, "KANEO_BOT_KEY")
		}
		if len(missing) != 0 {
			return ServeOptions{}, fmt.Errorf("kaneo connector is partially configured — missing %s", strings.Join(missing, ", "))
		}
		kaneo.WorkspaceID, _ = value("KANEO_WORKSPACE_ID")
		kaneo.BotActor, _ = value("KANEO_BOT_ACTOR")
	}

	herdrSocket, _ := value("HERDR_SOCKET_PATH")
	herdrSession, _ := value("HERDR_SESSION")
	herdrBin, _ := value("HERDR_BIN")

	return ServeOptions{
		Org:                 org,
		Target:              target,
		DataDir:             dataDir,
		DBPath:              dbPath,
		Port:                port,
		Bind:                bind,
		Socket:              strings.TrimSpace(socket),
		PromptTimeout:       promptTimeout,
		TickInterval:        tickInterval,
		RedeliverGrace:      redeliverGrace,
		RedeliverMaxBackoff: redeliverMaxBackoff,
		RedeliverReadFactor: readFactor,
		Connectors:          connectors,
		Shadow:              NewShadowMode(parseShadow(shadowRaw)),
		ShadowHeartbeat:     shadowHeartbeat,
		EnvelopePreview:     parseDefaultOn(previewRaw),
		DraftGuard:          parseDefaultOn(draftGuardRaw),
		DraftNotify:         parseDefaultOn(draftNotifyRaw),
		Herdr:               HerdrOptions{SocketPath: herdrSocket, Session: herdrSession, Bin: herdrBin},
		Mattermost:          MattermostOptions{Enabled: mmURL != "" && mmToken != "", URL: mmURL, BotToken: mmToken, AttachmentDir: mmAttachmentDir, BotUserID: mmBotUserID},
		Gmail:               GmailOptions{Enabled: gmailJSON != "" || gmailFile != "", AccountsJSON: gmailJSON, AccountsFile: gmailFile, AttachmentDir: gmailAttachmentDir, PollInterval: gmailPoll},
		Telegram:            telegram,
		Kaneo:               kaneo,
	}, nil
}

func LoadMCPOptions() (MCPOptions, error) {
	return mcpOptionsFromEnv(os.LookupEnv)
}

func mcpOptionsFromEnv(lookup envLookup) (MCPOptions, error) {
	agent, _, err := env(lookup, "AGENT")
	if err != nil {
		return MCPOptions{}, err
	}
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return MCPOptions{}, fmt.Errorf("COURIER_AGENT/CHANNEL_AGENT is required — it is the herdr agent name this session answers as")
	}
	hostURL, _, err := env(lookup, "HOST_URL")
	if err != nil {
		return MCPOptions{}, err
	}
	hostURL = strings.TrimRight(strings.TrimSpace(hostURL), "/")
	if hostURL == "" {
		hostURL = "http://127.0.0.1:8788"
	}
	cacheDir, _, err := env(lookup, "MANIFEST_CACHE_DIR")
	if err != nil {
		return MCPOptions{}, err
	}
	cacheDir = strings.TrimSpace(cacheDir)
	if cacheDir == "" {
		base, _ := lookup("XDG_CACHE_HOME")
		base = strings.TrimSpace(base)
		if base == "" {
			home, _ := lookup("HOME")
			base = filepath.Join(strings.TrimSpace(home), ".cache")
		}
		cacheDir = filepath.Join(base, "courier")
	}
	return MCPOptions{Agent: agent, HostURL: hostURL, ManifestCacheDir: cacheDir}, nil
}

// parseDefaultOn reads a knob whose safe value is on: an unset or unparsed value
// keeps the feature enabled, and only an explicit 0/false turns it off.
func parseDefaultOn(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false":
		return false
	default:
		return true
	}
}

func parseShadow(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "0", "false", "no":
		return false
	default:
		return true
	}
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func dualInt(get func(string) (string, bool, error), name string, fallback, min, max int) (int, error) {
	raw, set, err := get(name)
	if err != nil {
		return 0, err
	}
	if !set || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	return parseInt("COURIER_"+name+"/CHANNEL_"+name, strings.TrimSpace(raw), min, max)
}

func parseInt(name, raw string, min, max int) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("%s must be an integer from %d through %d", name, min, max)
	}
	return value, nil
}

func dualMilliseconds(get func(string) (string, bool, error), name string, fallback time.Duration, allowZero bool) (time.Duration, error) {
	raw, set, err := get(name)
	if err != nil {
		return 0, err
	}
	if !set || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || (!allowZero && value == 0) {
		return 0, fmt.Errorf("COURIER_%s/CHANNEL_%s must be a finite positive millisecond value", name, name)
	}
	return time.Duration(value * float64(time.Millisecond)), nil
}

// The TS tuning parser treats invalid and non-positive redelivery overrides as
// absent. Keep that typo-safe fallback while still surfacing dual-name conflicts.
func dualPositiveMillisecondsOrDefault(get func(string) (string, bool, error), name string, fallback time.Duration) (time.Duration, error) {
	raw, set, err := get(name)
	if err != nil {
		return 0, err
	}
	if !set || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, parseErr := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if parseErr != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
		return fallback, nil
	}
	return time.Duration(value * float64(time.Millisecond)), nil
}

func dualPositiveFloatOrDefault(get func(string) (string, bool, error), name string, fallback float64) (float64, error) {
	raw, set, err := get(name)
	if err != nil {
		return 0, err
	}
	if !set || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, parseErr := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if parseErr != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
		return fallback, nil
	}
	return value, nil
}

func exactSeconds(lookup envLookup, name string) (time.Duration, error) {
	raw, set := lookup(name)
	if !set || strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
		return 0, fmt.Errorf("%s must be a finite positive number of seconds", name)
	}
	return time.Duration(value * float64(time.Second)), nil
}
