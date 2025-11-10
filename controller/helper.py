import pandas as pd
import re

## setup format
num_re = re.compile(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?')
time_token_re = re.compile(r'^\d{1,2}:\d{2}$') 
CANON_MAP = {
    "co2": "CO2",
    "temperature": "Temperature",
    "humidity": "Humidity",
    "voltage": "Voltage",
    "rssi": "RSSI",
    "Temp Before Filter": "Temp Before Filter",
    "Diff Pressure": "Diff Pressure",
    "Fan Speed": "Fan Speed",
    "Duct Temperature": "Duct Temperature",
    "Duct Humidity": "Duct Humidity",
    "Duct CO2": "Duct CO2",
    "Duct VOC": "Duct VOC",
    "HLR Connect Status": "HLR Connect Status",
    "HLR Operation Mode": "HLR Operation Mode",
    "Switch-Interlock State": "Switch-Interlock State",
    "Switch-CO2 State": "Switch-CO2 State",
    "Co2 Level Scrub Mode": "Co2 Level Scrub Mode",
    "Co2 Level Enable Scrub Mode": "Co2 Level Enable Scrub Mode",
    "Interlock Status": "Interlock Status",
    "Clean Air Damper Open-Alarm": "Clean Air Damper Open-Alarm",
    "Exhaust Air Damper Open-Alarm": "Exhaust Air Damper Open-Alarm",
    "KM1 No Feedback-Alarm": "KM1 No Feedback-Alarm",
    "Fire-Alarm": "Fire-Alarm",
    "Service Door-Alarm": "Service Door-Alarm",
    "Fan-Alarm": "Fan-Alarm",
    "High Temperature-Alarm": "High Temperature-Alarm"
}


def parse_numeric(text: str):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    m = num_re.search(str(text))
    return float(m.group(0)) if m else None

def normalize_content(s: str) -> str:
    s = str(s)
    s = s.replace("，", ",") ## คอมม่าแบบจีน -> คอมม่าปกติ
    s = s.replace(",", ";") # ใช้ ; เป็นตัวคั่นมาตรฐาน
    s = s.replace("：", ":") # ฟูลวิธโคลอน -> โคลอนปกติ
    return s

def normalize_key(k: str) -> str:
    k = str(k).strip().lower()
    k = re.sub(r'\s+', ' ', k)
    k = k.replace('₂', '2')
    return k

def parse_content_row(s: str):
    out = {"content_time": None}
    if pd.isna(s):
        return out

    s = normalize_content(s)
    parts = [p.strip() for p in s.split(";") if p.strip()]
    if not parts:
        return out

    start_idx = 0
    if time_token_re.match(parts[0]):
        out["content_time"] = parts[0]
        start_idx = 1

    for p in parts[start_idx:]:
        if ":" not in p:
            continue
        key, val = p.split(":", 1)
        nkey = normalize_key(key)
        out[nkey] = parse_numeric(val)

    return out


def pretty_label_from_key(normalized_key: str) -> str:
    return normalized_key.capitalize()

def cleaning_data_scrub(df: pd.DataFrame):
    # หา Content + Report time แบบ case-insensitive
    content_col = next((c for c in df.columns if str(c).strip().lower() == "content"), None)
    if content_col is None:
        raise ValueError(f"Couldn't find a 'Content' column. Found: {list(df.columns)}")

    report_col = next((c for c in df.columns if str(c).strip().lower() == "report time"), None)
    has_report_time = report_col is not None

    parsed = df[content_col].apply(parse_content_row).apply(pd.Series)

    long_records = []
    for idx, row in parsed.iterrows():
        report_time_val = df.iloc[idx][report_col] if has_report_time else None
        for k, v in row.items():
            if k == "content_time":
                continue
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            canon_label = CANON_MAP.get(k, pretty_label_from_key(k))

            long_records.append({
                "row_index": idx,
                "report_time": report_time_val,
                "content_time": row.get("content_time"),
                "sensor_type": canon_label,
                "value": float(v),
            })

    df_extract = pd.DataFrame(long_records, columns=[
        "row_index", "report_time", "content_time", "sensor_type", "value"
    ])
    df_extract = df_extract.drop(columns=["content_time", "row_index"])

    return df_extract


def merged_scrub_function(df_full, df_extract):
    report_col_left = next((c for c in df_full.columns if str(c).strip().lower() == "report time"), None)
    if report_col_left is None:
        raise ValueError(f"df_bf_sc ไม่มีคอลัมน์ 'Report time' (found: {list(df_full.columns)})")

    report_col_right = next((c for c in df_extract.columns if str(c).strip().lower() == "report_time"), None)
    if report_col_right is None:
        raise ValueError(f"df_extract ไม่มีคอลัมน์ 'report_time' (found: {list(df_extract.columns)})")

    left = df_full.copy()
    right = df_extract.copy()
    left["_rt_key"]  = pd.to_datetime(left[report_col_left],  errors="coerce")
    right["_rt_key"] = pd.to_datetime(right[report_col_right], errors="coerce")

    merged_before_scrub = left.merge(
        right[["_rt_key", "sensor_type", "value"]],  
        on="_rt_key",
        how="left"
    ).drop(columns=["_rt_key"])

    merged_before_scrub["timestamp"] = (
        pd.to_datetime(merged_before_scrub["Report time"], errors="coerce").astype("int64") // 10**6)

    merged_before_scrub = merged_before_scrub[merged_before_scrub['sensor_type'] != "1"]
    # print(merged_before_scrub)
    return merged_before_scrub.drop(columns=['Content'])