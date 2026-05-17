const VERSION: &str = env!("CARGO_PKG_VERSION");

use log::{debug, info, error};
use scraper::{Html, Selector};
use influxdb::{Client, InfluxDbWriteable};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct Config {
    pv_webserver: PvWebserverConfig,
    influxdb: InfluxDbConfig,
    scraper: ScraperConfig,
}

#[derive(Debug, Deserialize)]
struct PvWebserverConfig {
    url: String,
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct InfluxDbConfig {
    url: String,
    database: String,
    token: String,
    measurement: String,
}

#[derive(Debug, Deserialize)]
struct ScraperConfig {
    interval_seconds: u64,
}

impl Config {
    fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}

#[derive(InfluxDbWriteable, Serialize, Deserialize, Debug, Clone)]
struct SolarMetrics {
    time: DateTime<Utc>,
    #[influxdb(tag)]
    device: String,
    ac_power_current: f64,
    total_energy: f64,
    daily_energy: f64,
    #[influxdb(tag)]
    status: String,
    string1_voltage: f64,
    string1_current: f64,
    string2_voltage: f64,
    string2_current: f64,
    l1_voltage: f64,
    l1_power: f64,
    l2_voltage: f64,
    l2_power: f64,
    l3_voltage: f64,
    l3_power: f64,
}

fn parse_float(s: &str) -> f64 {
    s.trim().replace(",", ".").parse::<f64>().unwrap_or(0.0)
}

fn extract_all_white_cells(html: &Html) -> Vec<String> {
    let selector = Selector::parse("td[bgcolor='#FFFFFF']").unwrap();
    html.select(&selector)
        .map(|el| el.text().collect::<String>().trim().to_string())
        .collect()
}

fn extract_status(html: &Html) -> String {
    let selector = Selector::parse("td").unwrap();
    let mut found_status = false;
    
    for element in html.select(&selector) {
        let text = element.text().collect::<String>().trim().to_string();
        
        if text == "Status" {
            found_status = true;
        } else if found_status && !text.is_empty() && !text.contains("&nbsp") {
            debug!("Found Status = {}", text);
            return text;
        }
    }
    "Unknown".to_string()
}

fn extract_device_name(html: &Html) -> String {
    // Der Device-Name steht nach "convert 8T dcs" im HTML
    // z.B. "Mertens_WR1 (255)"
    let selector = Selector::parse("font").unwrap();
    
    for element in html.select(&selector) {
        let text = element.text().collect::<String>();
        
        // Suche nach dem Pattern mit der Gerätenummer in Klammern
        if text.contains("convert 8T dcs") {
            debug!("Found 'convert 8T dcs' in text: {}", text);
            // Der Device-Name kommt nach mehreren Leerzeichen/Zeilenumbrüchen
            // Format: "convert 8T dcs\n    <br>\n                 \n      Mertens_WR1 (255)"
            let parts: Vec<&str> = text.split("convert 8T dcs").collect();
            if parts.len() > 1 {
                // Nimm den Teil nach "convert 8T dcs"
                let after_title = parts[1];
                // Entferne Leerzeichen und Zeilenumbrüche, finde das erste echte Wort
                let device_line = after_title
                    .lines()
                    .map(|l| l.trim())
                    .filter(|l| !l.is_empty() && *l != "<br>")
                    .next();
                
                if let Some(device_name) = device_line {
                    debug!("Found Device Name = {}", device_name);
                    // Extrahiere nur den Namen ohne die Nummer in Klammern
                    if let Some(name) = device_name.split('(').next() {
                        return name.trim().to_string();
                    }
                    return device_name.to_string();
                }
            }
        }
    }
    "Unknown_Device".to_string()
}

fn pending_queue_path() -> PathBuf {
    PathBuf::from("pending_metrics.jsonl")
}

fn append_pending_metric(metrics: &SolarMetrics) -> Result<(), Box<dyn std::error::Error>> {
    let path = pending_queue_path();
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    let mut writer = BufWriter::new(file);
    let json = serde_json::to_string(metrics)?;
    writer.write_all(json.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn load_pending_metrics() -> Result<Vec<SolarMetrics>, Box<dyn std::error::Error>> {
    let path = pending_queue_path();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = fs::File::open(&path)?;
    let reader = BufReader::new(file);
    let mut metrics = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<SolarMetrics>(&line) {
            Ok(entry) => metrics.push(entry),
            Err(err) => {
                error!("Skipping invalid pending metric entry: {}", err);
            }
        }
    }

    Ok(metrics)
}

fn write_pending_metrics(metrics: &[SolarMetrics]) -> Result<(), Box<dyn std::error::Error>> {
    let path = pending_queue_path();

    if metrics.is_empty() {
        if path.exists() {
            fs::remove_file(path)?;
        }
        return Ok(());
    }

    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)?;
    let mut writer = BufWriter::new(file);

    for metric in metrics {
        let json = serde_json::to_string(metric)?;
        writer.write_all(json.as_bytes())?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    Ok(())
}

async fn send_metric_to_influx(
    config: &Config,
    metrics: &SolarMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(&config.influxdb.url, &config.influxdb.database)
        .with_token(&config.influxdb.token);

    client
        .query(metrics.clone().try_into_query(&config.influxdb.measurement).unwrap())
        .await?;
    Ok(())
}

async fn flush_pending_metrics(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let pending = load_pending_metrics()?;
    if pending.is_empty() {
        return Ok(());
    }

    info!("Retrying {} pending metric(s)", pending.len());

    let mut unsent = Vec::new();
    for metric in pending {
        match send_metric_to_influx(config, &metric).await {
            Ok(_) => info!("Resent pending metric for {}", metric.time),
            Err(err) => {
                error!("Failed to resend pending metric: {}", err);
                unsent.push(metric);
            }
        }
    }

    write_pending_metrics(&unsent)?;
    if unsent.is_empty() {
        info!("All pending metrics were successfully sent")
    } else {
        error!("{} pending metric(s) remain in queue", unsent.len())
    }

    Ok(())
}

async fn scrape_and_save(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let resp = reqwest::Client::new()
        .get(&config.pv_webserver.url)
        .basic_auth(&config.pv_webserver.username, Some(&config.pv_webserver.password))
        .send()
        .await?;

    let body = resp.text().await?;
    debug!("Response Body (truncated): {}...", &body[..body.len().min(500)]);

    // HTML parsen
    let document = Html::parse_document(&body);
    let values = extract_all_white_cells(&document);
    
    debug!("Extracted values: {:?}", values);

    // Device-Namen aus dem HTML extrahieren
    let device_name = extract_device_name(&document);
    info!("Device: {}", device_name);
    
    let metrics = SolarMetrics {
        time: Utc::now(),
        device: device_name,
        ac_power_current: parse_float(values.get(0).unwrap_or(&"0".to_string())),
        total_energy: parse_float(values.get(1).unwrap_or(&"0".to_string())),
        daily_energy: parse_float(values.get(2).unwrap_or(&"0".to_string())),
        status: extract_status(&document),
        string1_voltage: parse_float(values.get(3).unwrap_or(&"0".to_string())),
        string1_current: parse_float(values.get(5).unwrap_or(&"0".to_string())),
        l1_voltage: parse_float(values.get(4).unwrap_or(&"0".to_string())),
        l1_power: parse_float(values.get(6).unwrap_or(&"0".to_string())),
        string2_voltage: parse_float(values.get(7).unwrap_or(&"0".to_string())),
        string2_current: parse_float(values.get(9).unwrap_or(&"0".to_string())),
        l2_voltage: parse_float(values.get(8).unwrap_or(&"0".to_string())),
        l2_power: parse_float(values.get(10).unwrap_or(&"0".to_string())),
        l3_voltage: parse_float(values.get(11).unwrap_or(&"0".to_string())),
        l3_power: parse_float(values.get(12).unwrap_or(&"0".to_string())),
    };

    info!("AC Power: {} W", metrics.ac_power_current);
    info!("Total Energy: {} kWh", metrics.total_energy);
    info!("Daily Energy: {} kWh", metrics.daily_energy);
    info!("Status: {}", metrics.status);
    info!("String 1: {} V / {} A", metrics.string1_voltage, metrics.string1_current);
    info!("String 2: {} V / {} A", metrics.string2_voltage, metrics.string2_current);
    info!("L1: {} V / {} W", metrics.l1_voltage, metrics.l1_power);
    info!("L2: {} V / {} W", metrics.l2_voltage, metrics.l2_power);
    info!("L3: {} V / {} W", metrics.l3_voltage, metrics.l3_power);

    // InfluxDB schreiben
    match send_metric_to_influx(config, &metrics).await {
        Ok(_) => info!("Successfully wrote data to InfluxDB"),
        Err(e) => {
            error!("Failed to write to InfluxDB: {}", e);
            if let Err(err) = append_pending_metric(&metrics) {
                error!("Failed to persist pending metric: {}", err);
            } else {
                info!("Stored metric locally for retry when InfluxDB is reachable")
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();

    info!("Solar Power Scrapper v{}", VERSION);

    // Config-Datei laden
    let config = match Config::from_file("config.toml") {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load config file: {}", e);
            error!("Please create a config.toml file in the application directory");
            return;
        }
    };

    info!("Config loaded successfully");
    debug!("PV Webserver: {}", config.pv_webserver.url);
    debug!("InfluxDB: {} / {}", config.influxdb.url, config.influxdb.database);
    info!("Scraping interval: {} seconds", config.scraper.interval_seconds);

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(config.scraper.interval_seconds));
    
    loop {
        interval.tick().await;
        
        info!("--- Starting scrape cycle ---");
        if let Err(e) = flush_pending_metrics(&config).await {
            error!("Error while flushing pending metrics: {}", e);
        }
        
        match scrape_and_save(&config).await {
            Ok(_) => info!("Scrape cycle completed successfully"),
            Err(e) => error!("Error during scrape cycle: {}", e),
        }
    }
}
