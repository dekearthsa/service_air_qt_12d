import pandas as pd
import re
pd.set_option('display.max_columns', None)
## setup format
num_re = re.compile(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?')
time_token_re = re.compile(r'^\d{1,2}:\d{2}$') 

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
    # return normalized_key.capitalize()
    return normalized_key.lower()

def adjust_co2(sensor_type, is_param, value):
    # print(sensor_type, is_param, value)
    if is_param.lower() == "co2" or is_param == "co2 level scrub mode" or is_param == "co2 level enable scrub mode":
        # print(sensor_type, is_param, value)
        if sensor_type == "Before Scrub":
            # print(55.215733 + (1.072297996 * value))
            return 55.215733 + (1.072297996 * value)
        elif sensor_type == "Interlock 4C":
            # print(16.238157 + (1.048766343 * value))
            return 16.238157 + (1.048766343 * value)
        elif sensor_type == "After Scrub":
            # print(52.831276 + (1.06400140 * value))
            return 52.831276 + (1.06400140 * value)
        else:
            return value
    else:
        return value

def convert_operation(asset_name, operation, v):
    
    if asset_name == "Interlock 4C":
        if operation == "hlr operation mode":
            # print(asset_name, operation, v)
            if int(v) == 0:
                return "manual_mode"
            elif int(v) == 1:
                return "standby_mode"
            elif int(v) == 2:
                return "scrubbing_mode"
            elif int(v) == 3:
                return "regen_mode"
            elif int(v) == 4:
                return "cooldown_mode"
            elif int(v) == 5:
                return "alarming"
            else: 
                return f"operation_code {v}"
        else:
            return "No operation detect"
    else:
        if asset_name == "Before Scrub":
            return "before_scrub"
        elif asset_name == "After Scrub":
            return "after_scrub"
        else:
            return "none"

def cleaning_data(df: pd.DataFrame):
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
            canon_label =pretty_label_from_key(k)
            # print(df.iloc[idx]["Asset name"] )
            data_convert = adjust_co2(df.iloc[idx]["Asset name"], canon_label, float(v))
            operation_name = convert_operation(df.iloc[idx]["Asset name"], canon_label, v)
            # print(operation_name)
            long_records.append({
                "row_index": idx,
                "report_time": report_time_val,
                "content_time": row.get("content_time"),
                "sensor_type": canon_label,
                "operation": operation_name,
                "value_raw": float(v),
                "value": float(data_convert),
            })

    df_extract = pd.DataFrame(long_records, columns=[
        "row_index", "report_time", "content_time", "sensor_type", "operation", "value_raw", "value"
    ])
    df_extract = df_extract.drop(columns=["content_time", "row_index"])

    return df_extract


def merged_function(df_full, df_extract):
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
        right[["_rt_key", "sensor_type", "operation","value_raw", "value"]],  
        on="_rt_key",
        how="left"
    ).drop(columns=["_rt_key"])

    merged_before_scrub["timestamp"] = (
        pd.to_datetime(merged_before_scrub["Report time"], errors="coerce").astype("int64") // 10**6)

    merged_before_scrub = merged_before_scrub[merged_before_scrub['sensor_type'] != "1"]
    # print("merged_before_scrub")
    # print(merged_before_scrub[merged_before_scrub['sensor_type'] == 'hlr operation mode'])
    return merged_before_scrub.drop(columns=['Content'])


def extract_columns(df: pd.DataFrame):
    select_sensor_type = ["co2","voc","temperature","humidity", "diff_pressure"]

    df["sensor_type"] = (
        df["sensor_type"]
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    df_wide = df.pivot_table(
        index='timestamp',           
        columns='sensor_type',     
        values='value',             
        aggfunc='first'            
    ).reset_index()

    select_sensor_type = ["co2","voc","temperature","humidity"]
    df = df[df["sensor_type"].isin(select_sensor_type)]
    # df = df.drop(columns=['id'])
    df_main = pd.merge(df, df_wide, on='timestamp',how='inner')
    df_main = df_main.drop_duplicates()
    if df_main['install_location'][0] == "Inlet":
        df_main = df_main.drop(columns=['co2','temperature', 'humidity', 'rssi','voc', 'voltage', 'version_number', 'diff_pressure'])
        return df_main
    else:
        df_main = df_main.drop(columns=['co2','temperature', 'humidity', 'rssi', 'voltage', 'version_number'])
        return df_main