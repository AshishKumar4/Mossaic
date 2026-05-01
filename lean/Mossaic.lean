-- Mossaic root module. Re-exports every namespace under Mossaic.Vfs and
-- Mossaic.Generated so `lake build` builds them all by default.
import Mossaic.Vfs.Common
import Mossaic.Vfs.Tenant
import Mossaic.Vfs.Refcount
import Mossaic.Vfs.Gc
import Mossaic.Vfs.AtomicWrite
import Mossaic.Vfs.Versioning
import Mossaic.Vfs.Encryption
import Mossaic.Vfs.Multipart
import Mossaic.Vfs.Quota
import Mossaic.Vfs.Preview
import Mossaic.Vfs.Tombstone
import Mossaic.Vfs.HistoryPreservation
import Mossaic.Vfs.StreamRouting
import Mossaic.Vfs.Cache
import Mossaic.Vfs.Yjs
import Mossaic.Vfs.ShareToken
import Mossaic.Vfs.RPC
import Mossaic.Vfs.PreviewToken
import Mossaic.Generated.ShardDO
import Mossaic.Generated.UserDO
import Mossaic.Generated.Placement
