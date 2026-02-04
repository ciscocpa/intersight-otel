use std::collections::BTreeMap;

use super::{IntersightMetric, IntersightMetricBatch, IntersightResourceMetrics};
use intersight_api::{Client, IntersightError};
use serde_json::Value;

pub async fn poll(
    client: &Client,
    query: &str,
    method: &Option<String>,
    body: &Option<String>,
    agg: &(dyn Aggregator + Sync + Send),
) -> Result<IntersightMetricBatch, PollerError> {
    let method = (*method).clone().unwrap_or_default();
    let method = method.as_str();

    let body = match body {
        Some(b) => b.as_str(),
        _ => "",
    };
    let body = match body {
        "" => Value::Null,
        _ => serde_json::from_str(body).map_err(|_| PollerError::ConfigError)?,
    };

    let response = match method {
        "post" => client
            .post(query, body)
            .await
            .map_err(PollerError::APIError)?,
        _ => client.get(query).await.map_err(PollerError::APIError)?,
    };

    let ret = agg.aggregate(response);

    Ok(ret)
}

#[derive(thiserror::Error, Debug)]
pub enum PollerError {
    #[error("error calling Intersight API")]
    APIError(IntersightError),

    #[error("poller configuration error")]
    ConfigError,
}

pub trait Aggregator {
    fn aggregate(&self, r: Value) -> IntersightMetricBatch;
}

//ResultCountingAggregator will explicitly count the number of results returned
pub struct ResultCountingAggregator {
    name: String,
}

impl ResultCountingAggregator {
    pub fn new(name: String) -> ResultCountingAggregator {
        ResultCountingAggregator { name }
    }
}

impl Aggregator for ResultCountingAggregator {
    fn aggregate(&self, r: Value) -> IntersightMetricBatch {
        let mut ret = IntersightResourceMetrics::default();

        let count: i64;
        if let serde_json::Value::Array(results) = &r["Results"] {
            count = results.len() as i64;
        } else {
            return vec![];
        }

        ret.metrics
            .push(IntersightMetric::new(&self.name, count as f64, None));

        vec![ret]
    }
}

//ResultCountAggregator will extract the "Count" field from the returned data
pub struct ResultCountAggregator {
    name: String,
}

impl ResultCountAggregator {
    pub fn new(name: String) -> ResultCountAggregator {
        ResultCountAggregator { name }
    }
}

impl Aggregator for ResultCountAggregator {
    fn aggregate(&self, r: Value) -> IntersightMetricBatch {
        let mut ret = IntersightResourceMetrics::default();

        let count: i64;
        if let serde_json::Value::Number(c) = &r["Count"] {
            if let Some(c) = c.as_i64() {
                count = c;
            } else {
                warn!("Unexpected type for result count");
                return vec![];
            }
        } else {
            warn!("'Count' field not present in API response. Did you mean to include '$count=true' in the API query?");
            return vec![];
        }

        ret.metrics
            .push(IntersightMetric::new(&self.name, count as f64, None));

        vec![ret]
    }
}

/// Extracts a numeric field from the first result and emits it as the metric value.
/// Useful for displaying values like memory_gb, cpu_cores in Splunk charts.
pub struct FirstResultNumericFieldAggregator {
    name: String,
    field: String,
    divisor: f64,
}

impl FirstResultNumericFieldAggregator {
    pub fn new(name: String, field: String, divisor: f64) -> FirstResultNumericFieldAggregator {
        FirstResultNumericFieldAggregator {
            name,
            field,
            divisor: if divisor == 0.0 { 1.0 } else { divisor },
        }
    }
}

impl Aggregator for FirstResultNumericFieldAggregator {
    fn aggregate(&self, r: Value) -> IntersightMetricBatch {
        let mut ret = IntersightResourceMetrics::default();

        // Get first result from Results array
        if let Value::Array(results) = &r["Results"] {
            if let Some(first) = results.first() {
                if let Value::Number(val) = &first[&self.field] {
                    if let Some(num) = val.as_f64() {
                        let value = num / self.divisor;
                        ret.metrics
                            .push(IntersightMetric::new(&self.name, value, None));
                        return vec![ret];
                    }
                }
            }
        }

        warn!(
            "Could not extract numeric field '{}' for metric '{}'",
            self.field, self.name
        );
        vec![]
    }
}

/// Extracts string fields from the first result and emits them as metric attributes.
/// Useful for extracting non-numeric values like model names, serial numbers, etc.
pub struct FirstResultFieldAggregator {
    name: String,
    fields: Vec<String>,
}

impl FirstResultFieldAggregator {
    pub fn new(name: String, fields: Vec<String>) -> FirstResultFieldAggregator {
        FirstResultFieldAggregator { name, fields }
    }
}

impl Aggregator for FirstResultFieldAggregator {
    fn aggregate(&self, r: Value) -> IntersightMetricBatch {
        let mut ret = IntersightResourceMetrics::default();
        let mut attributes = BTreeMap::new();

        // Get first result from Results array
        if let Value::Array(results) = &r["Results"] {
            if let Some(first) = results.first() {
                // Extract each requested field
                for field in &self.fields {
                    if let Value::String(val) = &first[field] {
                        attributes.insert(field.clone(), val.clone());
                    } else if let Value::Number(val) = &first[field] {
                        // Also handle numeric fields (e.g., VlanId, NumCores)
                        attributes.insert(field.clone(), val.to_string());
                    }
                }
            }
        }

        if attributes.is_empty() {
            warn!(
                "No fields extracted from API response for metric '{}'",
                self.name
            );
            return vec![];
        }

        // Emit metric with value=1 and extracted string attributes
        ret.metrics
            .push(IntersightMetric::new(&self.name, 1.0, Some(attributes)));

        vec![ret]
    }
}
