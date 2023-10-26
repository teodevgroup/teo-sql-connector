use bigdecimal::BigDecimal;
use chrono::{NaiveDate, Utc, DateTime, SecondsFormat};
use itertools::Itertools;
use teo_parser::r#type::Type;
use crate::schema::dialect::SQLDialect;
use teo_runtime::database::r#type::DatabaseType;
use teo_teon::Value;

pub trait ToSQLString {
    fn to_string(&self, dialect: SQLDialect) -> String;
}

pub trait TypeOrNull {
    fn or_null(&self, optional: bool) -> String;
}

impl TypeOrNull for &str {
    fn or_null(&self, optional: bool) -> String {
        self.to_string() + if optional { " or null" } else { "" }
    }
}

pub(crate) trait ValueToSQLString {
    fn to_sql_string<'a>(&self, r#type: &Type, optional: bool, dialect: SQLDialect) -> String;
    fn to_sql_string_array_arg<'a>(&self, r#type: &Type, optional: bool, dialect: SQLDialect) -> String;
}

impl ValueToSQLString for Value {

    fn to_sql_string<'a>(&self, r#type: &Type, optional: bool, dialect: SQLDialect) -> String {
        if optional {
            if self.is_null() {
                return "NULL".to_owned()
            }
        }
        match r#type {
            Type::String => ToSQLInputDialect::to_sql_input(&self.as_str().unwrap(), dialect),
            Type::Bool => self.as_bool().unwrap().to_sql_input(),
            Type::Int | Type::Int64 |
            Type::Float32 | Type::Float => if let Some(val) = self.as_float() {
                val.to_string()
            } else if let Some(val) = self.as_int() {
                val.to_string()
            } else {
                panic!("Uncoded number.")
            }
            Type::EnumVariant(_, _) => ToSQLInputDialect::to_sql_input(&self.as_str().unwrap(), dialect),
            Type::Array(element_field) => {
                let val = self.as_array().unwrap();
                let mut result: Vec<String> = vec![];
                for (_i, v) in val.iter().enumerate() {
                    result.push(v.to_sql_string(element_field.r#type(), element_field.is_optional(), dialect));
                }
                result.join(", ").wrap_in_array()
            }
            Type::Date => self.as_date().unwrap().to_string().to_sql_input(dialect),
            Type::DateTime => self.as_datetime().unwrap().to_string().to_sql_input(dialect),
            Type::Decimal => self.as_decimal().unwrap().to_string().to_sql_input(dialect),
            _ => { panic!() }
        }
    }

    fn to_sql_string_array_arg<'a>(&self, r#type: &Type, optional: bool, dialect: SQLDialect) -> String {
        if optional {
            if self.is_null() {
                return "NULL".to_owned()
            }
        }
        match r#type {
            Type::String => ToSQLInputDialect::to_sql_input(&self.as_str().unwrap(), dialect),
            Type::Bool => self.as_bool().unwrap().to_sql_input(),
            Type::Int | Type::Int64 |
            Type::Float32 | Type::Float => if let Some(val) = self.as_float() {
                val.to_string()
            } else if let Some(val) = self.as_int64() {
                val.to_string()
            } else {
                panic!("Uncoded number.")
            }
            Type::EnumVariant(_, _) => ToSQLInputDialect::to_sql_input(&self.as_str().unwrap(), dialect),
            Type::Array(element_field) => {
                let val = self.as_array().unwrap();
                let mut result: Vec<String> = vec![];
                for (_i, v) in val.iter().enumerate() {
                    result.push(v.to_sql_string_array_arg(element_field.r#type(), element_field.is_optional(), dialect));
                }
                result.join(",").wrap_in_array()
            }
            Type::Date => self.as_date().unwrap().to_string(),
            Type::DateTime => self.as_datetime().unwrap().to_string(),
            Type::Decimal => self.as_decimal().unwrap().to_string(),
            _ => { panic!() }
        }
    }
}

impl ValueToSQLString for &Value {
    fn to_sql_string<'a>(&self, r#type: &Type, optional: bool, dialect: SQLDialect) -> String {
        (*self).to_sql_string(r#type, optional, dialect)
    }

    fn to_sql_string_array_arg<'a>(&self, r#type: &Type, optional: bool, dialect: SQLDialect) -> String {
        (*self).to_sql_string_array_arg(r#type, optional, dialect)
    }
}

impl ToSQLString for &Value {
    fn to_string(&self, dialect: SQLDialect) -> String {
        match self {
            Value::Null => "NULL".to_owned(),
            Value::String(string) => string.to_sql_input(dialect),
            Value::Int(i) => i.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float32(i) => i.to_string(),
            Value::Float(i) => i.to_string(),
            Value::Bool(b) => b.to_sql_input(),
            Value::Date(d) => d.to_sql_input(dialect),
            Value::DateTime(d) => d.to_sql_input(dialect),
            Value::Decimal(d) => d.to_sql_input(dialect),
            Value::Array(values) => format!("array[{}]", values.iter().map(|v| ToSQLString::to_string(&v, dialect)).join(",")),
            Value::EnumVariant(e) => e.to_sql_input(dialect),
            _ => panic!("unhandled value: {:?}", self),
        }
    }
}

pub(crate) trait PSQLArrayToSQLString {
    fn to_string_with_ft(&self, dialect: SQLDialect, field_type: &Type) -> String;
}

fn field_type_to_psql(field_type: &Type) -> &'static str {
    match field_type {
        Type::Decimal => "decimal",
        Type::Int | Type::Int64 => "integer",
        Type::Float32 | Type::Float => "double precision",
        Type::String => "text",
        Type::Bool => "boolean",
        Type::Date => "date",
        Type::DateTime => "timestamp",
        _ => unreachable!(),
    }
}

impl PSQLArrayToSQLString for Value {
    fn to_string_with_ft(&self, dialect: SQLDialect, field_type: &Type) -> String {
        match self {
            Value::Array(values) => if values.is_empty() {
                format!("array[]::{}[]", field_type_to_psql(field_type.as_array().unwrap().r#type()))
            } else {
                format!("array[{}]", values.iter().map(|v| {
                    ToSQLString::to_string(&v, dialect)
                }).join(","))
            },
            _ => ToSQLString::to_string(&self, dialect),
        }
    }
}

pub trait ToWrapped {
    fn to_wrapped(&self) -> String;
}

impl ToWrapped for String {
    fn to_wrapped(&self) -> String {
        "(".to_owned() + self + ")"
    }
}

pub trait ToSQLInput {
    fn to_sql_input(&self) -> String;
}

pub trait ToSQLInputDialect {
    fn to_sql_input(&self, dialect: SQLDialect) -> String;
}

impl ToSQLInputDialect for String {
    fn to_sql_input(&self, dialect: SQLDialect) -> String {
        let mut result = String::with_capacity(self.len() + 2);
        result.push('\'');
        for ch in self.chars() {
            match ch {
                '\'' => if dialect.is_mysql() {
                    result.push_str("\\'");
                } else {
                    result.push_str("''");
                },
                _ => result.push(ch)
            }
        }
        result.push('\'');
        result
    }
}


impl ToSQLInputDialect for &str {
    fn to_sql_input(&self, dialect: SQLDialect) -> String {
        let mut result = String::with_capacity(self.len() + 2);
        result.push('\'');
        for ch in self.chars() {
            match ch {
                '\'' => if dialect.is_mysql() {
                    result.push_str("\\'");
                } else {
                    result.push_str("''");
                },
                _ => result.push(ch)
            }
        }
        result.push('\'');
        result
    }
}


impl ToSQLInput for bool {
    fn to_sql_input(&self) -> String {
        if *self { "TRUE".to_owned() } else { "FALSE".to_owned() }
    }
}

impl ToSQLInputDialect for BigDecimal {
    fn to_sql_input(&self, dialect: SQLDialect) -> String {
        let result = self.normalized().to_string();
        if dialect == SQLDialect::PostgreSQL {
            result + "::numeric"
        } else {
            result
        }
    }
}

impl ToSQLInputDialect for NaiveDate {
    fn to_sql_input(&self, dialect: SQLDialect) -> String {
        let result = self.format("%Y-%m-%d").to_string().to_sql_input(dialect);
        if dialect == SQLDialect::PostgreSQL {
            result + "::date"
        } else {
            result
        }
    }
}

impl ToSQLInputDialect for DateTime<Utc> {
    fn to_sql_input(&self, dialect: SQLDialect) -> String {
        if dialect == SQLDialect::SQLite {
            self.to_rfc3339_opts(SecondsFormat::Millis, true).to_sql_input(dialect)
        } else {
            let result = self.format("%Y-%m-%d %H:%M:%S.%3f").to_string().to_sql_input(dialect);
            if dialect == SQLDialect::PostgreSQL {
                result + "::timestamp"
            } else {
                result
            }
        }
    }
}

pub trait IfIMode {
    fn to_i_mode(&self, i_mode: bool) -> String;
}

impl IfIMode for &str {
    fn to_i_mode(&self, i_mode: bool) -> String {
        if i_mode {
            "LOWER(".to_owned() + self + ")"
        } else {
            self.to_string()
        }
    }
}

impl IfIMode for String {
    fn to_i_mode(&self, i_mode: bool) -> String {
        self.as_str().to_i_mode(i_mode)
    }
}

pub trait ToLike {
    fn to_like(&self, left: bool, right: bool) -> String;
}

impl ToLike for &str {
    fn to_like(&self, left: bool, right: bool) -> String {
        let mut retval = "".to_owned();
        retval.push(self.chars().nth(0).unwrap());
        if left {
            retval.push('%');
        }
        retval += &self[1..self.len() - 1];
        if right {
            retval.push('%');
        }
        retval.push(self.chars().nth(self.len() - 1).unwrap());
        retval
    }
}

impl ToLike for String {
    fn to_like(&self, left: bool, right: bool) -> String {
        self.as_str().to_like(left, right)
    }
}

pub trait WrapInArray {
    fn wrap_in_array(&self) -> String;
}

impl WrapInArray for &str {
    fn wrap_in_array(&self) -> String {
        "\'{".to_owned() + self + "}\'"
    }
}

impl WrapInArray for String {
    fn wrap_in_array(&self) -> String {
        self.as_str().wrap_in_array()
    }
}

pub trait SQLEscape {
    fn escape(&self, dialect: SQLDialect) -> String;
}

impl SQLEscape for &str {
    fn escape(&self, dialect: SQLDialect) -> String {
        match dialect {
            SQLDialect::MySQL => format!("`{}`", self),
            SQLDialect::PostgreSQL => format!("\"{}\"", self),
            _ => format!("`{}`", self),
        }
    }
}

impl SQLEscape for String {
    fn escape(&self, dialect: SQLDialect) -> String {
        match dialect {
            SQLDialect::MySQL => format!("`{}`", self),
            SQLDialect::PostgreSQL => format!("\"{}\"", self),
            _ => format!("`{}`", self),
        }
    }
}
