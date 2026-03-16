# ---------------------------------------------------------------------------
# Organization / Property classification
# ---------------------------------------------------------------------------

# O&O (Owned & Operated) organization IDs.
# Only these properties are reconciled against demand partner reports.
OWN_AND_OPERATED_ORG_IDS = {"si", "90min", "fs"}

# ---------------------------------------------------------------------------
# Network classification
# ---------------------------------------------------------------------------

# MinuteSSP is the only network with real (non-estimated) CPM in the events table.
# All other networks use estimated CPM and require reconciliation.
REAL_CPM_NETWORKS = {"MinuteSSP"}

# GAM network name as it appears in the events table.
# GAM events are reconciled using gam_data_transfer.
GAM_NETWORK = "GAM"

# ---------------------------------------------------------------------------
# SSP site_type classification
# ---------------------------------------------------------------------------

# These site_types in ssp_report are also captured in the events table
# (with better granularity). Events table is authoritative → exclude from SSP pipeline.
SSP_SITE_TYPES_IN_EVENTS = {"own_site", "own_player"}

# These site_types exist ONLY in ssp_report (not tracked in events).
# Must be included from SSP report to avoid revenue gaps.
SSP_SITE_TYPES_EXTERNAL_ONLY = {"external", "ext_player"}

# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------

# Revenue-generating event (ad impression served).
EVENT_SERVED = "served"

# Non-revenue events — tracked for counts only.
EVENT_PAGE_VIEW = "pageView"
EVENT_VIDEO_EMBED = "videoEmbed"

# Synthetic event type used for syndication revenue rows in the SSOT.
EVENT_SYNDICATION = "syndication"

# Synthetic event type used for SSP-external rows in the SSOT.
EVENT_SSP_EXTERNAL = "ssp_external"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_PATH = "db/minute_media.db"
