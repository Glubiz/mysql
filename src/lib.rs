use dotenvy::dotenv;
use lazy_static::lazy_static;
use mysql_async::{prelude::*, OptsBuilder, Pool};
use std::env;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

pub use mysql_async::prelude::FromRow;
pub use mysql_async::{params, FromRowError, Params, Row};

lazy_static! {
    static ref POOL: Arc<Mutex<Pool>> = {
        dotenv().ok();
        let database_url =
            url::Url::parse(&env::var("DATABASE_URL").expect("DATABASE_URL must be set"))
                .expect("Failed to parse DATABASE_URL");

        let user = database_url.username();
        let password = database_url.password().unwrap_or("");
        let host = database_url
            .host_str()
            .expect("DATABASE_URL must have a host");
        let database = database_url.path().trim_start_matches('/');

        let opts = OptsBuilder::default()
            .user(Some(user))
            .pass(Some(password))
            .ip_or_hostname(host)
            .db_name(Some(database));

        let pool = Pool::new(opts);

        Arc::new(Mutex::new(pool))
    };
}

pub async fn select<T: FromRow + Send>(
    query: &str,
    params_map: Option<Params>,
) -> Result<Vec<T>, Box<dyn Error>> {
    let pool = POOL.clone();
    let mut conn = pool.lock().await.get_conn().await?;
    match params_map {
        Some(params_map) => {
            match conn
                .exec_map(query, params_map, |row: Row| T::from_row(row))
                .await
            {
                Ok(result) => Ok(result),
                Err(err) => Err(Box::new(err)),
            }
        }
        None => match conn.exec_map(query, (), |row: Row| T::from_row(row)).await {
            Ok(result) => Ok(result),
            Err(err) => Err(Box::new(err)),
        },
    }
}

pub async fn execute(query: &str, params_map: Option<Params>) -> Result<(), Box<dyn Error>> {
    let pool = POOL.clone();
    let mut conn = pool.lock().await.get_conn().await?;

    match params_map {
        Some(params_map) => match conn.exec_drop(query, params_map).await {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        },
        None => match conn.exec_drop(query, ()).await {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        },
    }
}

pub trait Fetchable {
    fn fetch<T: FromRow + Send>(&self) -> Result<Vec<T>, Box<dyn Error>>;
}

pub trait Deletable {
    fn delete(&self) -> Result<(), Box<dyn Error>>;
}

pub trait Insertable {
    fn insert(&self) -> Result<(), Box<dyn Error>>;
}

pub trait Updatable {
    fn update(&self) -> Result<(), Box<dyn Error>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;
    use std::env;

    #[tokio::test]
    async fn test_select() {
        dotenv().ok();
        env::set_var("DATABASE_URL", "mysql://root:password@localhost/test");
        let query = "SELECT * FROM users WHERE id = :id";
        let params = params! {"id" => 1};
        let result: Vec<(i32, String)> = select(query, Some(params)).await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_execute() {
        dotenv().ok();
        env::set_var("DATABASE_URL", "mysql://root:password@localhost/test");
        let query = "UPDATE users SET name = :name WHERE id = :id";
        let params = params! {"name" => "NewName", "id" => 1};
        let result = execute(query, Some(params)).await;
        assert!(result.is_ok());
    }
}
