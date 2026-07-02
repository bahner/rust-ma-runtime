use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use ma_core::check_cap;
use tokio::sync::RwLock;
use tracing::debug;

pub use ma_core::{
    normalize_principal, AclMap, CapabilityEntry, CAP_CRUD, CAP_IPFS, CAP_RPC, GROUP_PREFIX,
};

/// The group principal that always holds wildcard capabilities in every in-memory ACL.
///
/// `+owners: ["*"]` is injected by [`inject_owners`] and [`load_acl_from_cid`]
/// at load time so it is visible when a user edits the ACL document. Documents
/// stored on IPFS do not need to include this entry.
///
/// At transport and CRUD gates the actual enforcement is done by [`is_owner`],
/// which checks the in-memory owner list directly without IPFS group resolution.
/// This guarantees that owners can never be locked out even if the ACL document
/// is empty or wrong.
pub const OWNERS_PRINCIPAL: &str = "+owners";

/// In-memory cache of named ACLs.
///
/// Key convention: `"acls.<name>"` → named ACL `AclMap`.
pub type AclCache = Arc<RwLock<HashMap<String, AclMap>>>;

/// Create a new empty [`AclCache`].
pub fn new_acl_cache() -> AclCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Always inject `+owners: ["*"]` into an [`AclMap`] loaded from a document.
///
/// This entry is purely cosmetic — it makes the owner group visible when
/// editing the ACL. Actual enforcement at transport and CRUD gates is done
/// by [`is_owner`], which bypasses `check_full` entirely for in-memory owners.
pub fn inject_owners(acl: &mut AclMap) {
    acl.insert(
        OWNERS_PRINCIPAL.to_string(),
        CapabilityEntry::from_caps(["*"]),
    );
}

/// Return `true` if `caller` is in the in-memory owners list.
///
/// Owners always pass transport and CRUD gates unconditionally — call this
/// before [`check_full`] to guarantee they can never be locked out.
pub fn is_owner(owners: &[String], caller: &str) -> bool {
    let normalized = normalize_principal(caller);
    owners.iter().any(|o| normalize_principal(o) == normalized)
}

/// Fetch an ACL document by CID from IPFS and deserialise it as [`AclMap`].
///
/// [`inject_owners`] is applied after deserialisation so `+owners: ["*"]`
/// is always present in the returned map.
pub async fn load_acl_from_cid(kubo_rpc_url: &str, cid: &str) -> Result<AclMap> {
    let mut acl = crate::kubo::dag_get::<AclMap>(kubo_rpc_url, cid)
        .await
        .with_context(|| format!("fetching ACL document {cid}"))?;
    inject_owners(&mut acl);
    Ok(acl)
}

/// Shared, mutable root transport-gate ACL.
///
/// An empty map means deny-all — nothing is allowed until a manifest or
/// owner explicitly sets an ACL.
pub type SharedAcl = Arc<RwLock<AclMap>>;

/// Create a new [`SharedAcl`] from an initial [`AclMap`].
pub fn new_shared_acl(map: AclMap) -> SharedAcl {
    Arc::new(RwLock::new(map))
}

/// Uniform async ACL check with group expansion.
///
/// Evaluation order:
/// 1. Explicit deny for `caller` or `"*"` → `Err` immediately (deny always wins).
/// 2. `check_cap(acl, caller, cap)` for each cap in `caps` — first `Ok` returns.
/// 3. For each `+group` key with `Deny`: expand members, caller in group? → `Err`.
/// 4. For each `+group` key with `Allow`: expand members, caller in group and has cap? → `Ok`.
/// 5. Nothing matched → `Err`.
///
/// `caps` uses OR semantics — the caller only needs to satisfy one.
///
/// Use `|_| async { Ok(vec![]) }` as `resolve_group` when group resolution
/// is not available (e.g. at the transport gate before the manifest loads).
pub async fn check_full<F, Fut>(
    acl: &AclMap,
    caller: &str,
    caps: &[&str],
    resolve_group: F,
) -> Result<()>
where
    F: Fn(&str) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<String>>>,
{
    let normalized = normalize_principal(caller);

    // Step 1 — explicit deny wins unconditionally.
    if matches!(acl.get(normalized), Some(CapabilityEntry::Deny)) {
        return Err(anyhow::anyhow!("access denied for {caller}"));
    }
    if matches!(acl.get("*"), Some(CapabilityEntry::Deny)) {
        return Err(anyhow::anyhow!(
            "access denied for {caller} (wildcard deny)"
        ));
    }

    // Step 2 — synchronous Allow check (handles principal entries + wildcard).
    for &cap in caps {
        if check_cap(acl, caller, cap).is_ok() {
            return Ok(());
        }
    }

    // Step 3 — group principal expansion (async).
    // 3a: deny groups checked first (deny wins).
    for (key, entry) in acl {
        if !key.starts_with(GROUP_PREFIX) || !matches!(entry, CapabilityEntry::Deny) {
            continue;
        }
        let members = resolve_group(key).await.unwrap_or_default();
        if members.iter().any(|m| normalize_principal(m) == normalized) {
            return Err(anyhow::anyhow!("access denied for {caller} (group {key})"));
        }
    }
    // 3b: allow groups.
    for (key, entry) in acl {
        if !key.starts_with(GROUP_PREFIX) {
            continue;
        }
        if let CapabilityEntry::Allow(cap_set) = entry {
            let members = resolve_group(key).await.unwrap_or_default();
            if members.iter().any(|m| normalize_principal(m) == normalized)
                && caps
                    .iter()
                    .any(|c| cap_set.contains(*c) || cap_set.contains("*"))
            {
                if key == OWNERS_PRINCIPAL {
                    debug!(caller = %caller, "{}", crate::i18n::t("acl-owners-access"));
                }
                return Ok(());
            }
        }
    }

    Err(anyhow::anyhow!(
        "access denied for {caller}: none of {caps:?} permitted"
    ))
}

/// Resolve a `+#<fragment>` group reference by dispatching `:contains` to the
/// local `ma-set` actor with the given `caller` DID.
///
/// Returns `vec![caller.to_string()]` if the set contains the caller,
/// or an empty vec if not (or if the actor is unavailable).
///
/// Group references of the form `+<handle>.<path>` (legacy IPFS-path style)
/// are not supported — use `+#<fragment>` to reference a local `ma-set` actor.
pub async fn query_actor_group(
    group_ref: &str,
    caller: &str,
    entity_registry: &crate::plugin::EntityRegistry,
) -> Result<Vec<String>> {
    let fragment = group_ref
        .strip_prefix("+#")
        .ok_or_else(|| anyhow::anyhow!("group ref must use +#<fragment> syntax: {group_ref}"))?;

    let ep = entity_registry
        .read()
        .await
        .get(fragment)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("group actor +#{fragment} not found in registry"))?;

    // Build CastInput for [:contains, caller] verb.
    let content = {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(
            &ciborium::Value::Array(vec![
                ciborium::Value::Text(":contains".into()),
                ciborium::Value::Text(caller.to_string()),
            ]),
            &mut buf,
        )
        .context("encoding :contains content")?;
        buf
    };
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let msg = crate::entity::LocalMessage {
        id: format!("acl-contains-{fragment}"),
        from: String::new(),
        to: format!("#{fragment}"),
        created_at: now_secs,
        expires: now_secs + 5, // 5 seconds
        reply_to: None,
        content_type: ma_core::CONTENT_TYPE_TERM.to_string(),
        content,
    };
    let input = crate::entity::CastInput { msg };
    let result = ep.handle_call(&input).await?;

    // Parse reply: :ok true → caller is member; anything else → not member.
    let contained = match ciborium::de::from_reader::<ciborium::Value, _>(result.output.as_slice())
    {
        Ok(ciborium::Value::Array(ref v)) => {
            v.first() == Some(&ciborium::Value::Text(":ok".into()))
                && v.get(1) == Some(&ciborium::Value::Bool(true))
        }
        Ok(ciborium::Value::Bool(b)) => b,
        _ => false,
    };

    if contained {
        Ok(vec![caller.to_string()])
    } else {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use ma_core::{check_cap, AclMap, CapabilityEntry, CAP_IPFS, CAP_RPC};

    fn allow(caps: &[&str]) -> CapabilityEntry {
        CapabilityEntry::from_caps(caps.iter().copied())
    }

    fn m(entries: &[(&str, CapabilityEntry)]) -> AclMap {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn wildcard_rpc_allows_rpc() {
        let acl = m(&[("*", allow(&["rpc"]))]);
        assert!(check_cap(&acl, "did:ma:alice", CAP_RPC).is_ok());
    }

    #[test]
    fn wildcard_rpc_denies_ipfs() {
        let acl = m(&[("*", allow(&["rpc"]))]);
        assert!(check_cap(&acl, "did:ma:alice", CAP_IPFS).is_err());
    }

    #[test]
    fn explicit_deny_wins_over_wildcard_allow() {
        let acl = m(&[
            ("*", allow(&["rpc", "ipfs"])),
            ("did:ma:bandit", CapabilityEntry::Deny),
        ]);
        assert!(check_cap(&acl, "did:ma:bandit", CAP_RPC).is_err());
    }

    #[test]
    fn exact_match_restricts_below_wildcard() {
        let acl = m(&[
            ("*", allow(&["rpc", "ipfs"])),
            ("did:ma:bob", allow(&["rpc"])),
        ]);
        assert!(check_cap(&acl, "did:ma:bob", CAP_RPC).is_ok());
        assert!(check_cap(&acl, "did:ma:bob", CAP_IPFS).is_err());
    }

    #[test]
    fn did_url_caller_is_normalized() {
        let acl = m(&[("did:ma:alice", allow(&["rpc", "ipfs"]))]);
        assert!(check_cap(&acl, "did:ma:alice#sign", CAP_RPC).is_ok());
    }

    #[test]
    fn default_deny_when_no_matching_entries() {
        assert!(check_cap(&AclMap::new(), "did:ma:alice", CAP_RPC).is_err());
    }

    #[tokio::test]
    async fn check_full_or_semantics() {
        // Caller has "write"; asking for ["read", "write"] — first matching cap wins.
        use super::check_full;
        let acl = m(&[("did:ma:alice", allow(&["write"]))]);
        let result = check_full(&acl, "did:ma:alice", &["read", "write"], |_| async {
            Ok(vec![])
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn check_full_direct_deny_beats_group_allow() {
        // Alice is directly denied AND a member of an allow group — direct deny wins.
        use super::check_full;
        let acl = m(&[
            ("did:ma:alice", CapabilityEntry::Deny),
            ("+carlotta.friends", allow(&["fortune"])),
        ]);
        let result = check_full(&acl, "did:ma:alice", &["fortune"], |g| {
            let g = g.to_string();
            async move {
                if g == "+carlotta.friends" {
                    Ok(vec!["did:ma:alice".to_string()])
                } else {
                    Ok(vec![])
                }
            }
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_full_groups_accumulate_caps() {
        // Alice is in two groups with different caps; she should have both.
        use super::check_full;
        let acl = m(&[
            ("+project.readers", allow(&["read"])),
            ("+project.writers", allow(&["write"])),
        ]);
        let resolve = |_g: &str| async move { Ok(vec!["did:ma:alice".to_string()]) };
        // Check read via readers group.
        let r1 = check_full(&acl, "did:ma:alice", &["read"], resolve).await;
        assert!(r1.is_ok());
        // Check write via writers group.
        let r2 = check_full(&acl, "did:ma:alice", &["write"], resolve).await;
        assert!(r2.is_ok());
        // Bob is not in any group.
        let r3 = check_full(&acl, "did:ma:bob", &["read"], |_| async { Ok(vec![]) }).await;
        assert!(r3.is_err());
    }

    #[tokio::test]
    async fn check_full_deep_group_path() {
        // Groups can have arbitrary depth: +alice.project4.admins
        use super::check_full;
        let acl = m(&[("+alice.project4.admins", allow(&["admin"]))]);
        let result = check_full(&acl, "did:ma:alice", &["admin"], |g| {
            let g = g.to_string();
            async move {
                if g == "+alice.project4.admins" {
                    Ok(vec!["did:ma:alice".to_string()])
                } else {
                    Ok(vec![])
                }
            }
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn check_full_allow_via_allow_entry() {
        use super::check_full;
        let acl = m(&[("did:ma:alice", allow(&["read"]))]);
        let result = check_full(&acl, "did:ma:alice", &["read"], |_| async { Ok(vec![]) }).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn check_full_deny_wins_over_wildcard_allow() {
        use super::check_full;
        let acl = m(&[
            ("*", allow(&["rpc", "ipfs"])),
            ("did:ma:bandit", CapabilityEntry::Deny),
        ]);
        let result = check_full(&acl, "did:ma:bandit", &["rpc"], |_| async { Ok(vec![]) }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_full_group_allow() {
        use super::check_full;
        let acl = m(&[("+carlotta.friends", allow(&["fortune"]))]);
        let result = check_full(&acl, "did:ma:alice", &["fortune"], |g| {
            let g = g.to_string();
            async move {
                if g == "+carlotta.friends" {
                    Ok(vec!["did:ma:alice".to_string()])
                } else {
                    Ok(vec![])
                }
            }
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn check_full_group_deny() {
        use super::check_full;
        let acl = m(&[("+alice.enemies", CapabilityEntry::Deny)]);
        let result = check_full(&acl, "did:ma:alice", &["fortune"], |g| {
            let g = g.to_string();
            async move {
                if g == "+alice.enemies" {
                    Ok(vec!["did:ma:alice".to_string()])
                } else {
                    Ok(vec![])
                }
            }
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_full_group_deny_wins_over_group_allow() {
        use super::check_full;
        let acl = m(&[
            ("+alice.enemies", CapabilityEntry::Deny),
            ("+alice.friends", allow(&["fortune"])),
        ]);
        // alice is in both groups — deny wins.
        let result = check_full(&acl, "did:ma:alice", &["fortune"], |g| {
            let _g = g.to_string();
            async move { Ok(vec!["did:ma:alice".to_string()]) }
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_full_group_non_member_denied() {
        use super::check_full;
        let acl = m(&[("+carlotta.friends", allow(&["fortune"]))]);
        let result = check_full(&acl, "did:ma:stranger", &["fortune"], |_| async {
            Ok(vec![])
        })
        .await;
        assert!(result.is_err());
    }
}
