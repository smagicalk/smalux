//! Persistence boundary for browser identities and sessions.
use super::{Role, SessionView, csrf, hash, hex, now_ms};
use crate::database::{DatabaseBackend, ServerDatabase};
use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement, TransactionTrait, Value};

pub(super) fn params(db: &ServerDatabase, sql: &str, values: Vec<Value>) -> Statement {
    let backend = match db.backend() {
        DatabaseBackend::Sqlite => DbBackend::Sqlite,
        DatabaseBackend::Postgres => DbBackend::Postgres,
        DatabaseBackend::MySql => DbBackend::MySql,
    };
    let sql = if backend == DbBackend::Postgres {
        let mut i = 0;
        sql.chars()
            .map(|c| {
                if c == '?' {
                    i += 1;
                    format!("${i}")
                } else {
                    c.to_string()
                }
            })
            .collect()
    } else {
        sql.to_owned()
    };
    Statement::from_sql_and_values(backend, sql, values)
}
pub(super) struct User {
    pub id: String,
    pub username: String,
    pub role: Role,
    pub enabled: bool,
    pub password_hash: String,
}
pub(super) async fn user(db: &ServerDatabase, username: &str) -> Result<Option<User>, DbErr> {
    let row = db
        .connection()
        .query_one_raw(params(
            db,
            "SELECT user_id,username,role,enabled,password_hash FROM web_users WHERE username=?",
            vec![username.into()],
        ))
        .await?;
    row.map(|r| {
        Ok(User {
            id: r.try_get("", "user_id")?,
            username: r.try_get("", "username")?,
            role: Role::parse(&r.try_get::<String>("", "role")?)
                .ok_or_else(|| DbErr::Custom("invalid stored role".into()))?,
            enabled: r.try_get("", "enabled")?,
            password_hash: r.try_get("", "password_hash")?,
        })
    })
    .transpose()
}
async fn audit<C: ConnectionTrait>(
    conn: &C,
    db: &ServerDatabase,
    action: &str,
    outcome: &str,
    uid: Option<&str>,
) -> Result<(), DbErr> {
    conn.execute_raw(params(
        db,
        "INSERT INTO web_auth_events(event_id,action,outcome,user_id,created_at) VALUES(?,?,?,?,?)",
        vec![
            uuid::Uuid::new_v4().to_string().into(),
            action.into(),
            outcome.into(),
            uid.map(str::to_owned).into(),
            now_ms().into(),
        ],
    ))
    .await?;
    Ok(())
}
pub(super) async fn denied(db: &ServerDatabase) -> Result<(), DbErr> {
    audit(db.connection(), db, "login", "rejected", None).await
}
pub(super) async fn bootstrap(
    db: &ServerDatabase,
    username: &str,
    phc: String,
) -> Result<(), DbErr> {
    let tx = db.connection().begin().await?;
    tx.execute_raw(params(
        db,
        "INSERT INTO web_bootstrap(claim) VALUES(?)",
        vec!["initial-admin".into()],
    ))
    .await?;
    let existing = tx
        .query_one_raw(params(db, "SELECT user_id FROM web_users LIMIT 1", vec![]))
        .await?;
    if existing.is_some() {
        return Err(DbErr::Custom("bootstrap already completed".into()));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let now = now_ms();
    tx.execute_raw(params(db,"INSERT INTO web_users(user_id,username,password_hash,role,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",vec![id.clone().into(),username.into(),phc.into(),"admin".into(),true.into(),now.into(),now.into()])).await?;
    audit(&tx, db, "bootstrap", "succeeded", Some(&id)).await?;
    tx.commit().await
}
pub(super) async fn create_session(
    db: &ServerDatabase,
    user: &User,
    secret: &[u8],
    absolute: i64,
    idle: i64,
) -> Result<SessionView, DbErr> {
    let tx = db.connection().begin().await?;
    let now = now_ms();
    let expires = now + absolute;
    let idle_expires = now + idle;
    let result=tx.execute_raw(params(db,"INSERT INTO web_sessions(session_id,secret_hash,user_id,created_at,last_seen_at,expires_at,idle_expires_at,revoked_at) SELECT ?,?,user_id,?,?,?,?,NULL FROM web_users WHERE user_id=? AND enabled=? AND password_hash=?",vec![uuid::Uuid::new_v4().to_string().into(),hex(&hash(secret)).into(),now.into(),now.into(),expires.into(),idle_expires.into(),user.id.clone().into(),true.into(),user.password_hash.clone().into()])).await?;
    if result.rows_affected() != 1 {
        return Err(DbErr::Custom(
            "identity changed during authentication".into(),
        ));
    }
    audit(&tx, db, "login", "succeeded", Some(&user.id)).await?;
    tx.commit().await?;
    Ok(SessionView {
        user_id: user.id.clone(),
        username: user.username.clone(),
        role: user.role,
        csrf_token: csrf(secret),
        expires_at_ms: expires,
        idle_expires_at_ms: idle_expires,
        permissions: permissions(user.role),
    })
}
pub(super) async fn session(
    db: &ServerDatabase,
    secret: &[u8],
    touch: bool,
    idle_ms: i64,
    absolute_ms: i64,
) -> Result<Option<SessionView>, DbErr> {
    let digest = hex(&hash(secret));
    let now = now_ms();
    let row=db.connection().query_one_raw(params(db,"SELECT s.user_id,u.username,u.role,s.expires_at,s.idle_expires_at,s.created_at FROM web_sessions s JOIN web_users u ON u.user_id=s.user_id WHERE s.secret_hash=? AND s.revoked_at IS NULL AND u.enabled=?",vec![digest.clone().into(),true.into()])).await?;
    let Some(r) = row else { return Ok(None) };
    let created: i64 = r.try_get("", "created_at")?;
    let expires = r
        .try_get::<i64>("", "expires_at")?
        .min(created.saturating_add(absolute_ms));
    let idle: i64 = r.try_get("", "idle_expires_at")?;
    if now >= expires || now >= idle {
        return Ok(None);
    }
    let role = Role::parse(&r.try_get::<String>("", "role")?)
        .ok_or_else(|| DbErr::Custom("invalid stored role".into()))?;
    let uid: String = r.try_get("", "user_id")?;
    let mut next_idle = idle.min(expires);
    if touch {
        next_idle = idle.max(now.saturating_add(idle_ms)).min(expires);
        let result=db.connection().execute_raw(params(db,"UPDATE web_sessions SET last_seen_at=CASE WHEN last_seen_at>? THEN last_seen_at ELSE ? END,idle_expires_at=CASE WHEN idle_expires_at>? THEN idle_expires_at ELSE ? END WHERE secret_hash=? AND revoked_at IS NULL AND expires_at>? AND idle_expires_at>? AND EXISTS(SELECT 1 FROM web_users WHERE user_id=? AND enabled=?)",vec![now.into(),now.into(),next_idle.into(),next_idle.into(),digest.into(),now.into(),now.into(),uid.clone().into(),true.into()])).await?;
        if result.rows_affected() != 1 {
            return Ok(None);
        }
    }
    Ok(Some(SessionView {
        user_id: uid,
        username: r.try_get("", "username")?,
        role,
        csrf_token: csrf(secret),
        expires_at_ms: expires,
        idle_expires_at_ms: next_idle,
        permissions: permissions(role),
    }))
}
fn permissions(role: Role) -> Vec<String> {
    let mut permissions = vec![
        "session.info".into(),
        "metrics.latest".into(),
        "stream.subscribe".into(),
        "agent.list".into(),
        "agent.get".into(),
    ];
    if matches!(role, Role::Admin | Role::Operator) {
        permissions.extend([
            "report.list".into(),
            "event.list".into(),
            "job.list".into(),
            "job.get".into(),
            "job.catalog.replace".into(),
            "operation.get".into(),
            "operation.list".into(),
        ]);
    }
    permissions
}
pub(super) async fn revoke(db: &ServerDatabase, secret: &[u8], uid: &str) -> Result<bool, DbErr> {
    let tx = db.connection().begin().await?;
    let now = now_ms();
    let result = tx
        .execute_raw(params(
            db,
            "UPDATE web_sessions SET revoked_at=? WHERE secret_hash=? AND revoked_at IS NULL",
            vec![now.into(), hex(&hash(secret)).into()],
        ))
        .await?;
    let revoked = result.rows_affected() == 1;
    if revoked {
        audit(&tx, db, "logout", "succeeded", Some(uid)).await?
    }
    tx.commit().await?;
    Ok(revoked)
}
pub(super) async fn cleanup(db: &ServerDatabase) -> Result<(), DbErr> {
    let now = now_ms();
    let sessions_before = now - 7 * 24 * 60 * 60 * 1000;
    let events_before = now - 30 * 24 * 60 * 60 * 1000;
    db.connection()
        .execute_raw(params(
            db,
            "DELETE FROM web_sessions WHERE expires_at<? OR idle_expires_at<? OR revoked_at<?",
            vec![
                sessions_before.into(),
                sessions_before.into(),
                sessions_before.into(),
            ],
        ))
        .await?;
    db.connection()
        .execute_raw(params(
            db,
            "DELETE FROM web_auth_events WHERE created_at<?",
            vec![events_before.into()],
        ))
        .await?;
    Ok(())
}
