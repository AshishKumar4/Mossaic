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
import Mossaic.Generated.ShardDO
import Mossaic.Generated.UserDO
import Mossaic.Generated.Placement
