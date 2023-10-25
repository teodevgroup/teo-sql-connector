use teo_runtime::database::mysql::r#type::MySQLType;
use teo_runtime::database::postgres::r#type::PostgreSQLType;
use teo_runtime::database::r#type::DatabaseType;
use teo_runtime::database::sqlite::r#type::SQLiteType;

pub trait DatabaseTypeToSQLString {

    fn to_sql_string(&self) -> String;
}

impl DatabaseTypeToSQLString for DatabaseType {

    fn to_sql_string(&self) -> String {
        match self {
            DatabaseType::Undetermined => panic!(),
            DatabaseType::MySQLType(t) => to_mysql_string(t),
            DatabaseType::PostgreSQLType(t) => to_postgres_string(t),
            DatabaseType::SQLiteType(t) => to_sqlite_string(t),
            DatabaseType::MongoDBType(_) => panic!(),
        }
    }
}

fn to_mysql_string(t: &MySQLType) -> String {
    match t {
        MySQLType::VarChar(len) => {
            let arg = format!("({})", len);
            // let charset = if let Some(v) = n {
            //     Cow::Owned(format!(" CHARACTER SET {v}"))
            // } else { Cow::Borrowed("") };
            // let collation = if let Some(v) = c {
            //     Cow::Owned(format!(" COLLATION {v}"))
            // } else { Cow::Borrowed("") };
            format!("VARCHAR{arg}")
            //format!("VARCHAR{arg}{charset}{collation}")
        }
        MySQLType::Text => "TEXT".to_string(),
        MySQLType::Char(len) => {
            let arg = format!("({})", len);
            format!("CHAR{arg}")
        },
        MySQLType::TinyText => "TINYTEXT".to_string(),
        MySQLType::MediumText => "MEDIUMTEXT".to_string(),
        MySQLType::LongText => "LONGTEXT".to_string(),
        MySQLType::Bit(len) => format!("BIT({})", len),
        MySQLType::TinyInt(len, signed) => {
            let suffix = if *signed { "" } else { " UNSIGNED" };
            format!("TINYINT({}){}", len, suffix)
        }
        MySQLType::Int(len, signed) => {
            let len = if let Some(len) = len { format!("({})", len) } else { "".to_owned() };
            let suffix = if *signed { "" } else { " UNSIGNED" };
            format!("INT({}){}", len, suffix)
        }
        MySQLType::SmallInt(len, signed) => {
            let len = if let Some(len) = len { format!("({})", len) } else { "".to_owned() };
            let suffix = if *signed { "" } else { " UNSIGNED" };
            format!("SMALLINT({}){}", len, suffix)
        }
        MySQLType::MediumInt(len, signed) => {
            let len = if let Some(len) = len { format!("({})", len) } else { "".to_owned() };
            let suffix = if *signed { "" } else { " UNSIGNED" };
            format!("MEDIUMINT({}){}", len, suffix)
        }
        MySQLType::BigInt(len, signed) => {
            let len = if let Some(len) = len { format!("({})", len) } else { "".to_owned() };
            let suffix = if *signed { "" } else { " UNSIGNED" };
            format!("BIGINT({}){}", len, suffix)
        }
        MySQLType::Year => panic!(),
        MySQLType::Float => "FLOAT".to_string(),
        MySQLType::Double => "DOUBLE".to_string(),
        MySQLType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        MySQLType::DateTime(fsp) => format!("DATATIME({})", fsp),
        MySQLType::Date => "DATE".to_string(),
        MySQLType::Time(_) => panic!(),
        MySQLType::Timestamp(p) => format!("TIMESTAMP({p})"),
        MySQLType::Json => "JSON".to_string(),
        MySQLType::LongBlob => "LONGBLOB".to_string(),
        MySQLType::Binary => "BINARY".to_string(),
        MySQLType::VarBinary => "VARBINARY".to_string(),
        MySQLType::TinyBlob => "TINYBLOB".to_string(),
        MySQLType::Blob => "BLOB".to_string(),
        MySQLType::MediumBlob => "MEDIUMBLOB".to_string(),
    }
}

fn to_postgres_string(t: &PostgreSQLType) -> String {
    match t {
        PostgreSQLType::Text => "TEXT".to_string(),
        PostgreSQLType::Char(len) => format!("CHAR({})", len),
        PostgreSQLType::VarChar(len) => format!("VARCHAR({})", len),
        PostgreSQLType::Bit(len) => format!("BIT({})", len),
        PostgreSQLType::VarBit => "BIT VARYING".to_string(),
        PostgreSQLType::UUID => "UUID".to_string(),
        PostgreSQLType::Xml => "XML".to_string(),
        PostgreSQLType::Inet => "INET".to_string(),
        PostgreSQLType::Boolean => "BOOLEAN".to_string(),
        PostgreSQLType::Integer => "INTEGER".to_string(),
        PostgreSQLType::SmallInt => "SMALLINT".to_string(),
        PostgreSQLType::Int => "INT".to_string(),
        PostgreSQLType::BigInt => "BIGINT".to_string(),
        PostgreSQLType::Oid => "OID".to_string(),
        PostgreSQLType::DoublePrecision => "DOUBLE PRECISION".to_string(),
        PostgreSQLType::Real => "REAL".to_string(),
        PostgreSQLType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
        PostgreSQLType::Money => "MONEY".to_string(),
        PostgreSQLType::Date => "DATE".to_string(),
        PostgreSQLType::Timestamp(p, tz) => {
            let tz = if tz { " WITH TIMEZONE" } else { "" };
            format!("TIMESTAMP({}){}", p, tz)
        }
        PostgreSQLType::Time(tz) => {
            let tz = if tz { " WITH TIMEZONE" } else { "" };
            format!("TIME{}", tz)
        }
        PostgreSQLType::Json => "JSON".to_string(),
        PostgreSQLType::JsonB => "JSONB".to_string(),
        PostgreSQLType::ByteA => "BYTEA".to_string(),
        PostgreSQLType::Array(inner) => to_postgres_string(inner.as_ref()) + "[]",
    }
}

fn to_sqlite_string(t: &SQLiteType) -> String {
    match t {
        SQLiteType::Text => "TEXT".to_string(),
        SQLiteType::Integer => "INTEGER".to_string(),
        SQLiteType::Real => "REAL".to_string(),
        SQLiteType::Decimal => "DECIMAL".to_string(),
        SQLiteType::Blob => "BLOB".to_string(),
    }
}

// DatabaseType::Enum(db_enum) => {
// let choices_vec: Vec<String> = db_enum.choices.iter().map(|c| format!("'{c}'")).collect();
// let choices = choices_vec.join(",");
// format!("ENUM ({choices})")
// }