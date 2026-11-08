//! Users, password hashes and vhost permissions, compatible with RabbitMQ's definitions.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use base64::Engine;
use parking_lot::RwLock;
use regex::Regex;
use sha2::{Digest, Sha256, Sha512};

use crate::error::{BrokerError, Result};

/// RabbitMQ's default hashing algorithm name.
pub const SHA256: &str = "rabbit_password_hashing_sha256";
/// RabbitMQ's SHA-512 hashing algorithm name.
pub const SHA512: &str = "rabbit_password_hashing_sha512";

/// Hashes a password the way RabbitMQ does: base64 of a 4-byte salt followed by
/// `sha256(salt ++ password)`.
pub fn hash_password(password: &str) -> String {
    let salt: [u8; 4] = rand::random();
    let mut h = Sha256::new();
    h.update(salt);
    h.update(password.as_bytes());
    let mut out = salt.to_vec();
    out.extend_from_slice(&h.finalize());
    base64::engine::general_purpose::STANDARD.encode(out)
}

/// Checks a password against a RabbitMQ password hash.
pub fn verify_password(password: &str, hash: &str, algorithm: &str) -> bool {
    let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(hash) else {
        return false;
    };
    if raw.len() < 5 {
        return false;
    }
    let (salt, expected) = raw.split_at(4);
    let actual: Vec<u8> = match algorithm {
        SHA512 => {
            let mut h = Sha512::new();
            h.update(salt);
            h.update(password.as_bytes());
            h.finalize().to_vec()
        }
        _ => {
            let mut h = Sha256::new();
            h.update(salt);
            h.update(password.as_bytes());
            h.finalize().to_vec()
        }
    };
    constant_time_eq(&actual, expected)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Which permission an operation needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Declare or delete queues and exchanges.
    Configure,
    /// Publish to an exchange, or bind to a queue or exchange.
    Write,
    /// Consume from a queue, or bind from an exchange.
    Read,
}

impl Access {
    pub fn as_str(&self) -> &'static str {
        match self {
            Access::Configure => "configure",
            Access::Write => "write",
            Access::Read => "read",
        }
    }
}

/// Configure, write and read patterns for one vhost.
#[derive(Debug, Clone)]
pub struct Permission {
    pub configure: String,
    pub write: String,
    pub read: String,
    configure_re: Regex,
    write_re: Regex,
    read_re: Regex,
}

fn compile(pattern: &str) -> Result<Regex> {
    // RabbitMQ treats the empty pattern as "^$": no access.
    let p = if pattern.is_empty() { "^$" } else { pattern };
    Regex::new(p).map_err(|e| {
        BrokerError::precondition_failed(format!("invalid permission pattern '{pattern}': {e}"))
    })
}

impl Permission {
    pub fn new(configure: &str, write: &str, read: &str) -> Result<Permission> {
        Ok(Permission {
            configure: configure.to_string(),
            write: write.to_string(),
            read: read.to_string(),
            configure_re: compile(configure)?,
            write_re: compile(write)?,
            read_re: compile(read)?,
        })
    }

    /// Full access, `.*` for everything.
    pub fn all() -> Permission {
        Permission::new(".*", ".*", ".*").expect("static patterns compile")
    }

    pub fn allows(&self, access: Access, resource: &str) -> bool {
        match access {
            Access::Configure => self.configure_re.is_match(resource),
            Access::Write => self.write_re.is_match(resource),
            Access::Read => self.read_re.is_match(resource),
        }
    }
}

/// A user. Immutable; changes replace the whole value.
#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub password_hash: String,
    pub hashing_algorithm: String,
    pub tags: Vec<String>,
    pub permissions: HashMap<String, Permission>,
}

impl User {
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }

    pub fn is_admin(&self) -> bool {
        self.has_tag("administrator")
    }

    pub fn can_access_vhost(&self, vhost: &str) -> bool {
        self.permissions.contains_key(vhost)
    }

    /// Checks one permission. `kind` is `queue` or `exchange` and only shapes the message.
    pub fn check(&self, vhost: &str, access: Access, kind: &str, resource: &str) -> Result<()> {
        // Publishing to the default exchange is checked as `amq.default`, like RabbitMQ.
        let resource = if kind == "exchange" && resource.is_empty() {
            "amq.default"
        } else {
            resource
        };
        let allowed = self
            .permissions
            .get(vhost)
            .is_some_and(|p| p.allows(access, resource));
        if allowed {
            Ok(())
        } else {
            Err(BrokerError::access_refused(format!(
                "{} access to {kind} '{resource}' in vhost '{vhost}' refused for user '{}'",
                access.as_str(),
                self.name
            )))
        }
    }
}

/// The user database.
#[derive(Debug, Default)]
pub struct Users {
    users: RwLock<BTreeMap<String, Arc<User>>>,
}

impl Users {
    pub fn get(&self, name: &str) -> Option<Arc<User>> {
        self.users.read().get(name).cloned()
    }

    pub fn list(&self) -> Vec<Arc<User>> {
        self.users.read().values().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.users.read().is_empty()
    }

    /// Checks a user's password. Errors use RabbitMQ's wording and never say whether the
    /// user exists.
    pub fn authenticate(&self, name: &str, password: &str, mechanism: &str) -> Result<Arc<User>> {
        let refused = || {
            BrokerError::access_refused(format!(
                "Login was refused using authentication mechanism {mechanism}. For details see the broker logfile."
            ))
        };
        let user = self.get(name).ok_or_else(refused)?;
        if user.password_hash.is_empty()
            || !verify_password(password, &user.password_hash, &user.hashing_algorithm)
        {
            return Err(refused());
        }
        Ok(user)
    }

    /// Creates or replaces a user. `password` of `None` keeps the current hash.
    pub fn put(&self, name: &str, password: Option<&str>, tags: Vec<String>) -> Arc<User> {
        self.put_hashed(
            name,
            password.map(|p| (hash_password(p), SHA256.to_string())),
            tags,
        )
    }

    /// Creates or replaces a user with an existing hash `(hash, algorithm)`.
    pub fn put_hashed(
        &self,
        name: &str,
        hash: Option<(String, String)>,
        tags: Vec<String>,
    ) -> Arc<User> {
        let mut users = self.users.write();
        let previous = users.get(name).cloned();
        let (password_hash, hashing_algorithm) = match (hash, &previous) {
            (Some(h), _) => h,
            (None, Some(p)) => (p.password_hash.clone(), p.hashing_algorithm.clone()),
            (None, None) => (String::new(), SHA256.to_string()),
        };
        let user = Arc::new(User {
            name: name.to_string(),
            password_hash,
            hashing_algorithm,
            tags,
            permissions: previous.map(|p| p.permissions.clone()).unwrap_or_default(),
        });
        users.insert(name.to_string(), user.clone());
        user
    }

    pub fn delete(&self, name: &str) -> bool {
        self.users.write().remove(name).is_some()
    }

    pub fn set_permission(&self, user: &str, vhost: &str, permission: Permission) -> Result<()> {
        let mut users = self.users.write();
        let current = users
            .get(user)
            .ok_or_else(|| BrokerError::not_found(format!("no user '{user}'")))?;
        let mut updated = (**current).clone();
        updated.permissions.insert(vhost.to_string(), permission);
        users.insert(user.to_string(), Arc::new(updated));
        Ok(())
    }

    pub fn clear_permission(&self, user: &str, vhost: &str) -> bool {
        let mut users = self.users.write();
        let Some(current) = users.get(user) else {
            return false;
        };
        let mut updated = (**current).clone();
        let removed = updated.permissions.remove(vhost).is_some();
        users.insert(user.to_string(), Arc::new(updated));
        removed
    }

    /// Removes every permission on a deleted vhost.
    pub fn clear_vhost(&self, vhost: &str) {
        let mut users = self.users.write();
        let names: Vec<String> = users.keys().cloned().collect();
        for n in names {
            let mut u = (*users[&n]).clone();
            if u.permissions.remove(vhost).is_some() {
                users.insert(n, Arc::new(u));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_verify_and_reject_wrong_passwords() {
        let h = hash_password("s3cret");
        assert!(verify_password("s3cret", &h, SHA256));
        assert!(!verify_password("wrong", &h, SHA256));
    }

    #[test]
    fn verifies_hashes_from_rabbitmq_definitions() {
        // The hash RabbitMQ 3.x writes for guest/guest with salt 0x908dc60a.
        let salt = [0x90u8, 0x8d, 0xc6, 0x0a];
        let mut h = Sha256::new();
        h.update(salt);
        h.update(b"guest");
        let mut raw = salt.to_vec();
        raw.extend_from_slice(&h.finalize());
        let hash = base64::engine::general_purpose::STANDARD.encode(raw);
        assert!(verify_password("guest", &hash, SHA256));
    }

    #[test]
    fn permissions_follow_rabbitmq_rules() {
        let users = Users::default();
        users.put("app", Some("pw"), vec![]);
        users
            .set_permission("app", "/", Permission::new("^app\\.", "", ".*").unwrap())
            .unwrap();
        let u = users.authenticate("app", "pw", "PLAIN").unwrap();
        assert!(u.check("/", Access::Configure, "queue", "app.jobs").is_ok());
        assert!(u.check("/", Access::Configure, "queue", "other").is_err());
        assert!(u.check("/", Access::Write, "exchange", "").is_err());
        assert!(u.check("/", Access::Read, "queue", "anything").is_ok());
        assert!(u.check("other-vhost", Access::Read, "queue", "x").is_err());
        assert!(users.authenticate("app", "nope", "PLAIN").is_err());
        assert!(users.authenticate("ghost", "pw", "PLAIN").is_err());
    }
}
