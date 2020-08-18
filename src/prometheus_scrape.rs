// Author: Casey Marshall (https://docs.rs/releases/casey-marshall)
// URL: https://docs.rs/crate/prometheus-scrape/0.1.1/source/src/lib.rs
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::io;

extern crate chrono;
use chrono::{DateTime, TimeZone, Utc};

extern crate regex;
use regex::Regex;

lazy_static! {
    static ref HELP_RE: Regex = Regex::new(r"^#\s+HELP\s+(\w+)\s+(.+)$").unwrap();
    static ref TYPE_RE: Regex = Regex::new(r"^#\s+TYPE\s+(\w+)\s+(\w+)").unwrap();
    static ref SAMPLE_RE: Regex = Regex::new(
        r"^(?P<name>\w+)(\{(?P<labels>[^}]+)\})?\s+(?P<value>\S+)(\s+(?P<timestamp>\S+))?"
    )
    .unwrap();
}

#[derive(Debug, Eq, PartialEq)]
pub enum LineInfo<'a> {
    Doc {
        metric_name: &'a str,
        doc: &'a str,
    },
    Type {
        metric_name: String,
        sample_type: SampleType,
    },
    Sample {
        metric_name: &'a str,
        labels: Option<&'a str>,
        value: &'a str,
        timestamp: Option<&'a str>,
    },
    Empty,
    Ignored,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SampleType {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

impl SampleType {
    pub fn parse(s: &str) -> SampleType {
        match s {
            "counter" => SampleType::Counter,
            "gauge" => SampleType::Gauge,
            "histogram" => SampleType::Histogram,
            "summary" => SampleType::Summary,
            _ => SampleType::Untyped,
        }
    }
}

impl<'a> LineInfo<'a> {
    pub fn parse(line: &'a str) -> LineInfo<'a> {
        let line = line.trim();
        if line.len() == 0 {
            return LineInfo::Empty;
        }
        match HELP_RE.captures(line) {
            Some(ref caps) => {
                return match (caps.get(1), caps.get(2)) {
                    (Some(ref metric_name), Some(ref doc)) => LineInfo::Doc {
                        metric_name: metric_name.as_str(),
                        doc: doc.as_str(),
                    },
                    _ => LineInfo::Ignored,
                }
            }
            None => {}
        }
        match TYPE_RE.captures(line) {
            Some(ref caps) => {
                return match (caps.get(1), caps.get(2)) {
                    (Some(ref metric_name), Some(ref sample_type)) => {
                        let sample_type = SampleType::parse(sample_type.as_str());
                        LineInfo::Type {
                            metric_name: match sample_type {
                                SampleType::Histogram => format!("{}_bucket", metric_name.as_str()),
                                _ => metric_name.as_str().to_string(),
                            },
                            sample_type: sample_type,
                        }
                    }
                    _ => LineInfo::Ignored,
                }
            }
            None => {}
        }
        match SAMPLE_RE.captures(line) {
            Some(ref caps) => {
                return match (
                    caps.name("name"),
                    caps.name("labels"),
                    caps.name("value"),
                    caps.name("timestamp"),
                ) {
                    (Some(ref name), labels, Some(ref value), timestamp) => LineInfo::Sample {
                        metric_name: name.as_str(),
                        labels: labels.map_or(None, |c| Some(c.as_str())),
                        value: value.as_str(),
                        timestamp: timestamp.map_or(None, |c| Some(c.as_str())),
                    },
                    _ => LineInfo::Ignored,
                }
            }
            None => LineInfo::Ignored,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Sample {
    pub metric: String,
    pub value: Value,
    pub labels: Labels,
    pub timestamp: DateTime<Utc>,
}

fn parse_bucket(s: &str, label: &str) -> Option<f64> {
    if let Some(kv) = s.split(",").next() {
        let kvpair = kv.split("=").collect::<Vec<_>>();
        let (k, v) = (kvpair[0], kvpair[1].trim_matches('"'));
        if k == label {
            match parse_golang_float(v) {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        } else {
            None
        }
    } else {
        None
    }
}

#[derive(Debug, PartialEq)]
pub struct HistogramCount {
    pub less_than: f64,
    pub count: f64,
}

#[derive(Debug, PartialEq)]
pub struct SummaryCount {
    pub quantile: f64,
    pub count: f64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Labels(pub HashMap<String, String>);

impl Labels {
    fn new() -> Labels {
        Labels(HashMap::new())
    }
    fn parse(s: &str) -> Labels {
        let mut l = HashMap::new();
        for kv in s.split(",") {
            let kvpair = kv.split("=").collect::<Vec<_>>();
            if kvpair.len() != 2 || kvpair[0].len() == 0 {
                continue;
            }
            l.insert(
                kvpair[0].to_string(),
                kvpair[1].trim_matches('"').to_string(),
            );
        }
        Labels(l)
    }
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(|ref x| x.as_str())
    }
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Counter(f64),
    Gauge(f64),
    Histogram(Vec<HistogramCount>),
    Summary(Vec<SummaryCount>),
    Untyped(f64),
}

impl Value {
    fn push_histogram(&mut self, h: HistogramCount) {
        match self {
            &mut Value::Histogram(ref mut hs) => hs.push(h),
            _ => {}
        }
    }
    fn push_summary(&mut self, s: SummaryCount) {
        match self {
            &mut Value::Summary(ref mut ss) => ss.push(s),
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct Scrape {
    pub docs: HashMap<String, String>,
    pub samples: Vec<Sample>,
}

fn parse_golang_float(s: &str) -> Result<f64, <f64 as std::str::FromStr>::Err> {
    match s.to_lowercase().as_str() {
        "nan" => Ok(std::f64::NAN), // f64::parse doesn't recognize 'nan'
        ref s => s.parse::<f64>(),  // f64::parse expects lowercase [+-]inf
    }
}

impl Scrape {
    pub fn parse(lines: impl Iterator<Item = io::Result<String>>) -> io::Result<Scrape> {
        Scrape::parse_at(lines, Utc::now())
    }
    pub fn parse_at(
        lines: impl Iterator<Item = io::Result<String>>,
        sample_time: DateTime<Utc>,
    ) -> io::Result<Scrape> {
        let mut docs: HashMap<String, String> = HashMap::new();
        let mut types: HashMap<String, SampleType> = HashMap::new();
        let mut buckets: HashMap<String, Sample> = HashMap::new();
        let mut samples: Vec<Sample> = vec![];

        for read_line in lines {
            let line = match read_line {
                Ok(line) => line,
                Err(e) => return Err(e),
            };
            match LineInfo::parse(&line) {
                LineInfo::Doc {
                    ref metric_name,
                    ref doc,
                } => {
                    docs.insert(metric_name.to_string(), doc.to_string());
                }
                LineInfo::Type {
                    ref metric_name,
                    ref sample_type,
                } => {
                    types.insert(metric_name.to_string(), *sample_type);
                }
                LineInfo::Sample {
                    metric_name,
                    ref labels,
                    value,
                    timestamp,
                } => {
                    // Parse value or skip
                    let fvalue = if let Ok(v) = parse_golang_float(value) {
                        v
                    } else {
                        continue;
                    };
                    // Parse timestamp or use given sample time
                    let timestamp = if let Some(Ok(ts_millis)) = timestamp.map(|x| x.parse::<i64>())
                    {
                        Utc.timestamp_millis(ts_millis)
                    } else {
                        sample_time
                    };
                    match (types.get(metric_name), labels) {
                        (Some(SampleType::Histogram), Some(labels)) => {
                            if let Some(lt) = parse_bucket(labels, "le") {
                                let sample =
                                    buckets.entry(metric_name.to_string()).or_insert(Sample {
                                        metric: metric_name.to_string(),
                                        labels: Labels::new(),
                                        value: Value::Histogram(vec![]),
                                        timestamp: timestamp,
                                    });
                                sample.value.push_histogram(HistogramCount {
                                    less_than: lt,
                                    count: fvalue,
                                })
                            }
                        }
                        (Some(SampleType::Summary), Some(labels)) => {
                            if let Some(q) = parse_bucket(labels, "quantile") {
                                let sample =
                                    buckets.entry(metric_name.to_string()).or_insert(Sample {
                                        metric: metric_name.to_string(),
                                        labels: Labels::new(),
                                        value: Value::Summary(vec![]),
                                        timestamp: timestamp,
                                    });
                                sample.value.push_summary(SummaryCount {
                                    quantile: q,
                                    count: fvalue,
                                })
                            }
                        }
                        (ty, labels) => samples.push(Sample {
                            metric: metric_name.to_string(),
                            labels: labels.map_or(Labels::new(), |x| Labels::parse(x)),
                            value: match ty {
                                Some(SampleType::Counter) => Value::Counter(fvalue),
                                Some(SampleType::Gauge) => Value::Gauge(fvalue),
                                _ => Value::Untyped(fvalue),
                            },
                            timestamp: timestamp,
                        }),
                    };
                }
                _ => {}
            }
        }
        samples.extend(buckets.drain().map(|(_k, v)| v).collect::<Vec<_>>());
        Ok(Scrape {
            docs: docs,
            samples: samples,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufRead;

    #[test]
    fn test_lineinfo_parse() {
        assert_eq!(
            LineInfo::parse("foo 2"),
            LineInfo::Sample {
                metric_name: "foo",
                value: "2",
                labels: None,
                timestamp: None,
            }
        );
        assert_eq!(
            LineInfo::parse("foo wtf -1"),
            LineInfo::Sample {
                metric_name: "foo",
                value: "wtf",
                labels: None,
                timestamp: Some("-1"),
            }
        );
        assert_eq!(LineInfo::parse("foo=2"), LineInfo::Ignored,);
        assert_eq!(
            LineInfo::parse("foo 2 1543182234"),
            LineInfo::Sample {
                metric_name: "foo",
                value: "2",
                labels: None,
                timestamp: Some("1543182234"),
            }
        );
        assert_eq!(
            LineInfo::parse("foo{bar=baz} 2 1543182234"),
            LineInfo::Sample {
                metric_name: "foo",
                value: "2",
                labels: Some("bar=baz"),
                timestamp: Some("1543182234"),
            }
        );
        assert_eq!(
            LineInfo::parse("foo{bar=baz,quux=nonce} 2 1543182234"),
            LineInfo::Sample {
                metric_name: "foo",
                value: "2",
                labels: Some("bar=baz,quux=nonce"),
                timestamp: Some("1543182234"),
            }
        );
        assert_eq!(
            LineInfo::parse("# HELP foo this is a docstring"),
            LineInfo::Doc {
                metric_name: "foo",
                doc: "this is a docstring"
            },
        );
        assert_eq!(
            LineInfo::parse("# TYPE foobar bazquux"),
            LineInfo::Type {
                metric_name: "foobar".to_string(),
                sample_type: SampleType::Untyped,
            },
        );
    }

    fn pair_to_string(pair: &(&str, &str)) -> (String, String) {
        (pair.0.to_string(), pair.1.to_string())
    }

    #[test]
    fn test_labels_parse() {
        assert_eq!(
            Labels::parse("foo=bar"),
            Labels([("foo", "bar")].iter().map(pair_to_string).collect())
        );
        assert_eq!(
            Labels::parse("foo=bar,"),
            Labels([("foo", "bar")].iter().map(pair_to_string).collect())
        );
        assert_eq!(
            Labels::parse(",foo=bar,"),
            Labels([("foo", "bar")].iter().map(pair_to_string).collect())
        );
        assert_eq!(
            Labels::parse("=,foo=bar,"),
            Labels([("foo", "bar")].iter().map(pair_to_string).collect())
        );
        assert_eq!(
            Labels::parse(r#"foo="bar""#),
            Labels([("foo", "bar")].iter().map(pair_to_string).collect())
        );
        assert_eq!(
            Labels::parse(r#"foo="bar",baz="quux""#),
            Labels(
                [("foo", "bar"), ("baz", "quux")]
                    .iter()
                    .map(pair_to_string)
                    .collect()
            )
        );
        assert_eq!(
            Labels::parse(r#"foo="foo bar",baz="baz quux""#),
            Labels(
                [("foo", "foo bar"), ("baz", "baz quux")]
                    .iter()
                    .map(pair_to_string)
                    .collect()
            )
        );
        assert_eq!(Labels::parse("==="), Labels(HashMap::new()),);
    }

    #[test]
    fn test_golang_float() {
        assert_eq!(parse_golang_float("1.0"), Ok(1.0f64));
        assert_eq!(parse_golang_float("-1.0"), Ok(-1.0f64));
        assert!(parse_golang_float("NaN").unwrap().is_nan());
        assert_eq!(parse_golang_float("Inf"), Ok(std::f64::INFINITY));
        assert_eq!(parse_golang_float("+Inf"), Ok(std::f64::INFINITY));
        assert_eq!(parse_golang_float("-Inf"), Ok(std::f64::NEG_INFINITY));
    }

    #[test]
    fn test_parse_samples() {
        let scrape = r#"
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"}    3 1395066363000

# Escaping in label values:
msdos_file_access_time_seconds{path="C:\\DIR\\FILE.TXT",error="Cannot find file:\n\"FILE.TXT\""} 1.458255915e9

# Minimalistic line:
metric_without_timestamp_and_labels 12.47

# A weird metric from before the epoch:
something_weird{problem="division by zero"} +Inf -3982045

# A histogram, which has a pretty complex representation in the text format:
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.2"} 100392
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320

# Finally a summary, which has a complex representation, too:
# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
rpc_duration_seconds{quantile="0.5"} 4773
rpc_duration_seconds{quantile="0.9"} 9001
rpc_duration_seconds{quantile="0.99"} 76656
rpc_duration_seconds_sum 1.7560473e+07
rpc_duration_seconds_count 2693
"#;
        let br = io::BufReader::new(scrape.as_bytes());
        let s = Scrape::parse(br.lines()).unwrap();
        assert_eq!(s.samples.len(), 11);

        fn assert_match_sample<'a, F>(samples: &'a Vec<Sample>, f: F) -> &'a Sample
        where
            for<'r> F: FnMut(&'r &'a Sample) -> bool,
        {
            samples.iter().filter(f).next().as_ref().unwrap()
        }
        assert_eq!(
            assert_match_sample(&s.samples, |s| s.metric == "http_requests_total"
                && s.labels.get("code") == Some("200")),
            &Sample {
                metric: "http_requests_total".to_string(),
                value: Value::Counter(1027f64),
                labels: Labels(
                    [("method", "post"), ("code", "200")]
                        .iter()
                        .map(pair_to_string)
                        .collect()
                ),
                timestamp: Utc.timestamp_millis(1395066363000),
            }
        );
        assert_eq!(
            assert_match_sample(&s.samples, |s| s.metric == "http_requests_total"
                && s.labels.get("code") == Some("400")),
            &Sample {
                metric: "http_requests_total".to_string(),
                value: Value::Counter(3f64),
                labels: Labels(
                    [("method", "post"), ("code", "400")]
                        .iter()
                        .map(pair_to_string)
                        .collect()
                ),
                timestamp: Utc.timestamp_millis(1395066363000),
            }
        );
    }
}
