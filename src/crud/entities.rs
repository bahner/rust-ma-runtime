use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use std::sync::Arc;
use tracing::info;

use crate::acl::check_full;
use crate::entity::{EntityNode, IpldLink};

use super::helpers::{
    load_manifest, resolve_ipfs_ref, send_crud_data_cbor, send_crud_error, send_crud_i18n_error,
    send_crud_i18n_errorf, send_crud_ok, send_crud_ok_cid, send_crud_reply_cbor,
    spawn_entity_reload, with_manifest_crud,
};
use super::CrudHandlerCtx;

// ── Management capability helpers ─────────────────────────────────────────────

async fn check_entity_management_cap(
    message: &ma_core::Message,
    ctx: &CrudHandlerCtx,
    caps: &[&str],
) -> Result<()> {
    // Snapshot and drop the read guard before the async check_full call.
    // Holding the guard across an await would block any concurrent write
    // to root_acl (e.g. :acl: update) until the check completes.
    let acl = ctx.root_acl.read().await.clone();
    check_full(&acl, &message.from, caps, |key| {
        let name = key.strip_prefix('+').unwrap_or(key).to_string();
        async move {
            Ok(ctx
                .group_cache
                .read()
                .await
                .get(&name)
                .cloned()
                .unwrap_or_default())
        }
    })
    .await
    .with_context(|| {
        format!(
            "entity management denied for {}: requires {:?}",
            message.from, caps
        )
    })
}

// ── Entities handler ─────────────────────────────────────────────────────────

pub(super) async fn handle_entities_ns(
    message: &ma_core::Message,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
) -> Result<()> {
    match rest.len() {
        0 => match (tail, args.as_slice()) {
            (None, []) => {
                info!("{}", crate::i18n::t("root-list-entities"));
                let names: Vec<String> = ctx.entity_registry.read().await.keys().cloned().collect();
                send_crud_data_cbor(message, reply_type, ctx, &names).await
            }
            (Some(""), _) => {
                send_crud_i18n_error(message, reply_type, ctx, "refuse-delete-root").await
            }
            _ => Err(anyhow!("unknown entities operation")),
        },
        1 => handle_single_entity(message, &rest[0], tail, args, reply_type, ctx).await,
        2.. => {
            handle_entity_field(message, &rest[0], &rest[1..], tail, args, reply_type, ctx).await
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_single_entity(
    message: &ma_core::Message,
    name: &String,
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
) -> Result<()> {
    match (tail, args.as_slice()) {
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            let Some(link) = manifest.entities.get(name.as_str()) else {
                return send_crud_error(message, reply_type, ctx, "entity-not-found").await;
            };
            let entity: EntityNode = crate::kubo::dag_get(&ctx.kubo_rpc_url, &link.cid).await?;
            send_crud_data_cbor(message, reply_type, ctx, &entity).await
        }
        (Some(""), []) => {
            // Delete entity — requires `delete` + `entities` in root ACL.
            check_entity_management_cap(message, ctx, &["delete", "entities"]).await?;
            let name = name.as_str();
            let manifest = load_manifest(ctx).await?;
            if !manifest.entities.contains_key(name) {
                return send_crud_error(message, reply_type, ctx, "entity-not-found").await;
            }
            ctx.entity_registry.write().await.remove(name);
            let new_root = with_manifest_crud(ctx, |m| {
                m.entities.remove(name);
                Ok(())
            })
            .await?;
            info!(name = %name, cid = %new_root, "{}", crate::i18n::t("entity-deleted"));
            send_crud_ok(message, reply_type, ctx).await
        }
        (Some(""), [CborValue::Text(raw)]) => {
            // Upsert entity — caller needs the entity's `kind` as a capability in root ACL.
            // The kind is read from the EntityNode itself; no separate state required.
            let name = name.as_str();
            if name.chars().any(char::is_control) {
                return send_crud_i18n_error(message, reply_type, ctx, "entity-name-invalid").await;
            }
            if crate::entity::RESERVED_ENTITY_NAMES.contains(&name) {
                return send_crud_i18n_errorf(
                    message,
                    reply_type,
                    ctx,
                    "reserved-entity-name",
                    &[("name", name)],
                )
                .await;
            }
            let Some(cid) = resolve_ipfs_ref(&ctx.kubo_rpc_url, raw).await? else {
                return send_crud_i18n_error(message, reply_type, ctx, "cidv1-required").await;
            };
            let cid = cid.as_str();
            let mut entity_node: EntityNode = crate::kubo::dag_get(&ctx.kubo_rpc_url, cid)
                .await
                .with_context(|| format!("fetching entity node from {cid}"))?;
            // ACL gate: caller must hold the entity's kind protocol ID as a capability.
            check_entity_management_cap(message, ctx, &[entity_node.kind.as_str()]).await?;
            // Genesis rule (hardcoded, cross-cutting — see
            // `entity::is_genesis_entity`'s doc comment): entity-level
            // `attributes.genesis` overrides the kind's own, merged at
            // read time. Either way it's true, only owners may create the
            // instance, and it always gets `parent: None`, regardless of
            // what the caller's published EntityNode requested.
            //
            // Prefer the hydrated in-memory kind registry, with a manifest/IPFS
            // fallback for stale or externally-mutated roots.
            let cached_kind = ctx
                .kind_registry
                .read()
                .await
                .get(entity_node.kind.as_str())
                .cloned();
            let kind_node = if let Some(k) = cached_kind {
                Some(k.as_ref().clone())
            } else {
                let manifest = load_manifest(ctx).await?;
                if let Some(link) = manifest.kinds.get_protocol(entity_node.kind.as_str()) {
                    let raw_kind: crate::entity::KindNode =
                        crate::kubo::dag_get(&ctx.kubo_rpc_url, &link.cid)
                            .await
                            .with_context(|| {
                                format!("fetching kind node for '{}'", entity_node.kind)
                            })?;
                    let resolved = if raw_kind.extends.is_some() {
                        crate::entity::resolve_kind_extends(&ctx.kubo_rpc_url, &manifest, raw_kind)
                            .await?
                    } else {
                        raw_kind
                    };
                    Some(resolved)
                } else {
                    None
                }
            };
            if let Some(kind_node) = &kind_node {
                if crate::entity::is_genesis_entity(kind_node, &entity_node) {
                    let owners = ctx.stats.read().await.owners.clone();
                    if !crate::acl::is_owner(&owners, &message.from) {
                        return send_crud_i18n_error(
                            message,
                            reply_type,
                            ctx,
                            "genesis-kind-owner-only",
                        )
                        .await;
                    }
                    entity_node.parent = None;
                }
            }
            with_manifest_crud(ctx, |m| {
                m.entities.insert(name.to_string(), IpldLink::new(cid));
                Ok(())
            })
            .await?;
            let runtime_config = {
                let manifest = load_manifest(ctx).await?;
                let cfg = ctx.shared_config.read().await;
                super::config::public_plugin_config(&manifest, &cfg)
            };
            spawn_entity_reload(
                name.to_string(),
                entity_node,
                ctx.kind_registry.clone(),
                ctx.stats.clone(),
                Arc::clone(&ctx.kubo_rpc_url),
                Arc::clone(&ctx.our_did),
                ctx.envelope_tx.clone(),
                ctx.entity_registry.clone(),
                ctx.avatar_key,
                ctx.manifest_writer.clone(),
                runtime_config,
            );
            info!(name = %name, cid = %cid, "{}", crate::i18n::t("entity-created"));
            send_crud_ok_cid(message, reply_type, ctx, cid).await
        }
        _ => Err(anyhow!("unknown entities.{name} operation")),
    }
}

// ── Entity field helpers ───────────────────────────────────────────────────────

pub(super) async fn fetch_entity_node(ctx: &CrudHandlerCtx, name: &str) -> Result<EntityNode> {
    let manifest = load_manifest(ctx).await?;
    let link = manifest
        .entities
        .get(name)
        .ok_or_else(|| anyhow!("entity not found: {name}"))?;
    crate::kubo::dag_get(&ctx.kubo_rpc_url, &link.cid)
        .await
        .with_context(|| format!("fetching entity {name} from {}", link.cid))
}

pub(super) async fn update_entity_node(
    ctx: &CrudHandlerCtx,
    name: &str,
    entity: &EntityNode,
) -> Result<String> {
    let entity_cid = crate::kubo::dag_put(&ctx.kubo_rpc_url, entity)
        .await
        .with_context(|| format!("publishing updated entity {name}"))?;
    with_manifest_crud(ctx, |m| {
        m.entities
            .insert(name.to_string(), IpldLink::new(&entity_cid));
        Ok(())
    })
    .await?;
    Ok(entity_cid)
}

async fn handle_entity_field(
    message: &ma_core::Message,
    name: &String,
    field_path: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
) -> Result<()> {
    let Some((field, sub_path)) = field_path.split_first() else {
        return Err(anyhow!("empty field path in entity.{name}"));
    };

    // Generic GET — works for any leaf field without field-specific code.
    if tail.is_none() && args.is_empty() && sub_path.is_empty() {
        let entity = fetch_entity_node(ctx, name).await?;
        let mut entity_cbor = Vec::new();
        ciborium::ser::into_writer(&entity, &mut entity_cbor)
            .context("serializing entity for field GET")?;
        let cbor_map: CborValue = ciborium::de::from_reader(entity_cbor.as_slice())
            .context("re-parsing entity CBOR map")?;
        if let CborValue::Map(entries) = cbor_map {
            if let Some((_, value)) = entries
                .into_iter()
                .find(|(k, _)| matches!(k, CborValue::Text(s) if s == field))
            {
                return send_crud_data_cbor(message, reply_type, ctx, &value).await;
            }
        }
        return Err(anyhow!("field '{field}' not found in entity '{name}'"));
    }

    match field.as_str() {
        "acl" => {
            handle_entity_acl_field(message, name, sub_path, tail, args, reply_type, ctx).await
        }
        _ => Err(anyhow!("unknown entity field '{field}' in entity.{name}")),
    }
}

async fn handle_entity_acl_field(
    message: &ma_core::Message,
    name: &String,
    sub_path: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
) -> Result<()> {
    if !sub_path.is_empty() {
        return Err(anyhow!(
            "entity field 'acl' sub-path '{}' not yet implemented",
            sub_path.join(".")
        ));
    }
    match (tail, args.as_slice()) {
        (None, []) => {
            let entity = fetch_entity_node(ctx, name).await?;
            let ipfs_path = format!("/ipfs/{}", entity.acl);
            send_crud_reply_cbor(message, reply_type, ctx, &CborValue::Text(ipfs_path)).await
        }
        (Some(""), [CborValue::Text(acl_name)]) => {
            let manifest = load_manifest(ctx).await?;
            if !manifest.acls.contains_key(acl_name) {
                let available: Vec<&String> = manifest.acls.keys().collect();
                return Err(anyhow!(
                    "ACL name '{acl_name}' not found in manifest; available: {available:?}"
                ));
            }
            let mut entity = fetch_entity_node(ctx, name).await?;
            entity.acl = acl_name.clone();
            let entity_cid = update_entity_node(ctx, name, &entity).await?;
            info!(name = %name, acl_name = %acl_name, entity_cid = %entity_cid, "entity ACL name set");
            send_crud_ok_cid(message, reply_type, ctx, &entity_cid).await
        }
        (Some(""), []) => {
            let mut entity = fetch_entity_node(ctx, name).await?;
            entity.acl = String::new();
            let entity_cid = update_entity_node(ctx, name, &entity).await?;
            info!(name = %name, entity_cid = %entity_cid, "entity ACL cleared");
            send_crud_ok_cid(message, reply_type, ctx, &entity_cid).await
        }
        _ => Err(anyhow!("unknown entities.{name}.acl operation")),
    }
}
