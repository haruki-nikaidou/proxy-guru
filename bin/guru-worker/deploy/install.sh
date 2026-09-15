#!/bin/sh
# guru-worker agent installer.
#
# Served by the control plane's nginx next to the binary it installs, and run
# on a data-plane host through the one-liner the dashboard renders:
#
#   curl -fsSL https://<master>/agent/install.sh | sudo env GURU_… sh
#
# Idempotent: re-running it for the same instance repairs or upgrades that
# instance in place. Everything it needs arrives in the environment — never on
# the command line, which `ps` shows to every user on the host.
#
#   GURU_MASTER          what the worker dials, e.g. https://guru.example.com
#   GURU_SERVER_ID       the orchestration_server record key
#   GURU_API_KEY         the server's own agent key (gs_…) or an operator API key
#   GURU_UNIT            the systemd instance: guru-worker@<GURU_UNIT>
#   GURU_AGENT_VERSION   the published worker version, e.g. 0.2.0-beta
#   GURU_AGENT_SHA256    lowercase hex SHA-256 of that binary
#   GURU_DOWNLOAD_BASE   optional; where the artifacts are (default: $GURU_MASTER/agent)
#   GURU_LOG_LEVEL       optional; default info
set -eu

PREFIX=/opt/guru-worker
ETC=/etc/guru-worker
UNIT=/etc/systemd/system/guru-worker@.service
SVC_USER=guru-worker

die() { printf 'guru-worker install: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is not installed"; }
fetch() { curl -fsSL --retry 3 --retry-delay 1 -o "$2" "$1" || die "download failed: $1"; }

[ "$(id -u)" -eq 0 ] || die "run as root (the dashboard's one-liner uses sudo)"
for name in GURU_MASTER GURU_SERVER_ID GURU_API_KEY GURU_UNIT GURU_AGENT_VERSION GURU_AGENT_SHA256; do
    eval "value=\${$name:-}"
    [ -n "$value" ] || die "$name is not set"
done
case "$GURU_UNIT" in
    ''|-*|*[!a-z0-9-]*) die "GURU_UNIT must match ^[a-z0-9][a-z0-9-]*\$ (got '$GURU_UNIT')" ;;
esac
case "$GURU_AGENT_VERSION" in
    ''|*/*|.*) die "GURU_AGENT_VERSION is not a version (got '$GURU_AGENT_VERSION')" ;;
esac
need curl; need sha256sum; need useradd; need systemctl; need install; need mktemp
[ "$(uname -m)" = x86_64 ] || die "the published binary is x86_64 (glibc); this host is $(uname -m)"
BASE="${GURU_DOWNLOAD_BASE:-$GURU_MASTER/agent}"

# 1. The service user and the install tree it may write to (self-update swaps
#    `current` there), plus the root-owned config directory.
if ! id "$SVC_USER" >/dev/null 2>&1; then
    useradd --system --user-group --home-dir /nonexistent --shell /usr/sbin/nologin "$SVC_USER"
fi
install -d -m 0755 -o "$SVC_USER" -g "$SVC_USER" "$PREFIX"
install -d -m 0750 -o root -g "$SVC_USER" "$ETC"

# 2. The binary: downloaded next to where it will live, verified, then
#    installed under its version and made `current`. The version that was
#    current stays on disk as `previous`.
tmp="$(mktemp "$PREFIX/.download.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
fetch "$BASE/$GURU_AGENT_VERSION/guru-worker" "$tmp"
printf '%s  %s\n' "$GURU_AGENT_SHA256" "$tmp" | sha256sum -c --status \
    || die "SHA-256 mismatch for $BASE/$GURU_AGENT_VERSION/guru-worker"
install -d -m 0755 -o "$SVC_USER" -g "$SVC_USER" "$PREFIX/$GURU_AGENT_VERSION"
install -m 0755 -o "$SVC_USER" -g "$SVC_USER" "$tmp" "$PREFIX/$GURU_AGENT_VERSION/guru-worker"
"$PREFIX/$GURU_AGENT_VERSION/guru-worker" --version >/dev/null \
    || die "the downloaded binary does not run on this host"
if [ -L "$PREFIX/current" ] && [ "$(readlink "$PREFIX/current")" != "$GURU_AGENT_VERSION" ]; then
    ln -sfn "$(readlink "$PREFIX/current")" "$PREFIX/previous"
fi
ln -sfn "$GURU_AGENT_VERSION" "$PREFIX/current"

# 3. The environment file: the only place the worker's settings live. It holds
#    the key, so it is readable by root and the service group only.
umask 077
cat >"$ETC/$GURU_UNIT.env.tmp" <<EOF
GURU_MASTER=$GURU_MASTER
GURU_SERVER_ID=$GURU_SERVER_ID
GURU_API_KEY=$GURU_API_KEY
GURU_STATE_DIR=/var/lib/guru-worker/$GURU_UNIT
GURU_LOG_LEVEL=${GURU_LOG_LEVEL:-info}
EOF
umask 022
chown root:"$SVC_USER" "$ETC/$GURU_UNIT.env.tmp"
chmod 0640 "$ETC/$GURU_UNIT.env.tmp"
mv "$ETC/$GURU_UNIT.env.tmp" "$ETC/$GURU_UNIT.env"

# 4. The template unit, refreshed from the same publication as the binary.
fetch "$BASE/guru-worker@.service" "$UNIT.tmp"
chmod 0644 "$UNIT.tmp" && mv "$UNIT.tmp" "$UNIT"

# 5. Up. `restart` rather than `start`, so a re-run picks up the new binary and
#    environment; `enable` so it survives a reboot.
systemctl daemon-reload
systemctl enable "guru-worker@$GURU_UNIT" >/dev/null 2>&1
systemctl restart "guru-worker@$GURU_UNIT"
echo "guru-worker@$GURU_UNIT: $GURU_AGENT_VERSION installed, following $GURU_MASTER as server $GURU_SERVER_ID"
echo "  systemctl status guru-worker@$GURU_UNIT"
echo "  journalctl -u guru-worker@$GURU_UNIT -f"
