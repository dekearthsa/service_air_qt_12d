from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import pandas as pd
from controller.helper import cleaning_data_scrub, merged_scrub_function

DB_PATH = "sensor_data_projectD.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS sensor_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_type TEXT,
    asset_number TEXT,
    asset_name TEXT,
    system TEXT,
    install_location TEXT,
    device_type TEXT,
    device_id TEXT,
    project TEXT,
    report_time TEXT,           
    timestamp INTEGER,         
    sensor_type TEXT,
    value REAL
);
""")
conn.commit()
conn.close()
print("✅ Table 'sensor_data' created successfully.")

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route("/debug", methods=["GET"])
def RouteDebug():
    return "Hello, World!"

@app.route("/upload", methods=["POST"])
def RouteUploadExcel():
    try:
        # 1) รับไฟล์
        f = request.files.get("file")
        if f is None or f.filename == "":
            return jsonify({"ok": False, "error": "no file (form field 'file')"}), 400

        # 2) อ่านเป็น DataFrame (รองรับ xlsx/xls/csv)
        filename = f.filename.lower()
        if filename.endswith((".xlsx", ".xls")):
            df_full = pd.read_excel(f)  # sheet แรก
        elif filename.endswith(".csv"):
            df_full = pd.read_csv(f)
        else:
            return jsonify({"ok": False, "error": "unsupported file type; use .xlsx/.xls/.csv"}), 415

        # 3) แตกค่า content → long (sensor_type,value,report_time)
        df_extract = cleaning_data_scrub(df_full)

        # 4) รวมกลับกับ df_full และสร้าง timestamp (ms) ภายในฟังก์ชัน
        merged = merged_scrub_function(df_full, df_extract)

        # ---------- เตรียมข้อมูลสำหรับ insert ----------
        # ฟังก์ชันช่วยหา column แบบ case-insensitive
        def find_col(df, *cands):
            cands_l = [c.strip().lower() for c in cands]
            for col in df.columns:
                if str(col).strip().lower() in cands_l:
                    return col
            return None

        col_map = {
            "data_type": find_col(merged, "data type", "datatype"),
            "asset_number": find_col(merged, "asset number", "asset_number"),
            "asset_name": find_col(merged, "asset name", "asset_name"),
            "system": find_col(merged, "system"),
            "install_location": find_col(merged, "install location", "install_location"),
            "device_type": find_col(merged, "device type", "device_type"),
            "device_id": find_col(merged, "device id", "device_id"),
            "project": find_col(merged, "project", "project id", "project_id"),
            "report_time": find_col(merged, "report time", "report_time"),
            "timestamp": "timestamp" if "timestamp" in merged.columns else None,
            "sensor_type": "sensor_type" if "sensor_type" in merged.columns else None,
            "value": "value" if "value" in merged.columns else None,
        }

        df_insert = pd.DataFrame(index=merged.index)
        for db_col, src_col in col_map.items():
            if src_col is None:
                df_insert[db_col] = None
            else:
                df_insert[db_col] = merged[src_col]

        # if df_insert["timestamp"].isna().any():
        #     if col_map["report_time"] is not None:
        #         ts = pd.to_datetime(df_insert["report_time"], errors="coerce")
        #         df_insert.loc[df_insert["timestamp"].isna(), "timestamp"] = (ts.view("int64") // 10**6)

        # df_insert["timestamp"] = pd.to_numeric(df_insert["timestamp"], errors="coerce").astype("Int64")
        df_insert["value"] = pd.to_numeric(df_insert["value"], errors="coerce")

        before = len(df_insert)
        df_insert = df_insert.dropna(subset=["sensor_type", "value", "timestamp"])
        after = len(df_insert)

        rows = list(
            df_insert[[
                "data_type", "asset_number", "asset_name", "system", "install_location",
                "device_type", "device_id", "project", "report_time", "timestamp",
                "sensor_type", "value"
            ]].itertuples(index=False, name=None)
        )

        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.executemany("""
                INSERT INTO sensor_data (
                    data_type, asset_number, asset_name, system, install_location,
                    device_type, device_id, project, report_time, timestamp,
                    sensor_type, value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()

        return jsonify({
            "ok": True,
            "received_rows": len(merged),
            "prepared_rows": before,
            "inserted_rows": after,
            "skipped_rows": before - after
        }), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# /get?start=10000&end=20000
@app.route("/get", methods=["GET"])
def RouteGet():
    try:
        startDate = request.args.get('start')
        endDate = request.args.get('end')
        print(startDate, endDate)
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT * FROM sensor_data
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
            """, (int(startDate), int(endDate)))  
            rows = cur.fetchall()
            return jsonify({"ok": True, "rows": rows}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3012, debug=True)