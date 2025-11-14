from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import pandas as pd
from controller.helper import cleaning_data, merged_function, extract_columns

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
    operation TEXT,
    value_raw REAL,
    value REAL,
    clean_air_damper_open_alarm REAL,
    co2_level_enable_scrub_mode REAL,
    co2_level_scrub_mode REAL,
    exhaust_air_damper_open_alarm REAL,
    fan_alarm REAL,
    fan_speed REAL,
    fire_alarm REAL,
    high_temperature_alarm REAL,
    hlr_connect_status REAL,
    hlr_operation_mode REAL,
    interlock_status REAL,
    km1_no_feedback_alarm REAL,
    service_door_alarm REAL,
    switch_co2_state REAL,
    switch_interlock_state REAL,
    temp_before_filter REAL
);
""")
conn.commit()
conn.close()
print("✅ Table 'sensor_data' created successfully.")

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route("/backend_c/debug", methods=["GET"])
def RouteDebug():
    return "Hello, World!"

@app.route("/backend_c/upload", methods=["POST"])
def RouteUploadExcel():
    # try:
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
        df_extract = cleaning_data(df_full)
        # print("df_extract")
        # print(df_extract[df_extract['sensor_type']== 'hlr operation mode'], "\n")
        # 4) รวมกลับกับ df_full และสร้าง timestamp (ms) ภายในฟังก์ชัน
        merged = merged_function(df_full, df_extract)

        # ---------- เตรียมข้อมูลสำหรับ insert ----------
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
            "operation": "operation" if "operation" in merged.columns else None,
            "value_raw": "value_raw" if "value_raw" in  merged.columns else None,
            "value": "value" if "value" in merged.columns else None,
        }

        df_insert = pd.DataFrame(index=merged.index)
        
        for db_col, src_col in col_map.items():
            if src_col is None:
                df_insert[db_col] = None
            else:
                df_insert[db_col] = merged[src_col]

        df_insert["value"] = pd.to_numeric(df_insert["value"], errors="coerce")
        df_insert["value_raw"] = pd.to_numeric(df_insert["value_raw"], errors="coerce")
        

        ## drop columns ที่ไม่มีข้อมูล  operation
        before = len(df_insert)
        df_insert = df_insert.dropna(subset=["sensor_type", "operation","value_raw",  "value", "timestamp"])
        after = len(df_insert)  
        print(df_insert[df_insert['sensor_type']== 'hlr operation mode'], "\n")
        df_insert.loc[df_insert['sensor_type'] == 'duct co2', 'sensor_type'] = 'co2'
        df_insert.loc[df_insert['sensor_type'] == 'duct temperature', 'sensor_type'] = 'temperature'
        df_insert.loc[df_insert['sensor_type'] == 'duct humidity', 'sensor_type'] = 'humidity'
        df_insert.loc[df_insert['sensor_type'] == 'duct voc', 'sensor_type'] = 'voc'
        # print("drop columns ที่ไม่มีข้อมูล  operation")
        # print(df_insert[df_insert['sensor_type']== 'hlr operation mode'], "\n")
        ## แปลงเป็น array tuple เพื่อให้เตรียมใส่ใน sqlite 
        df_insert = extract_columns(df_insert)
        print("df_insert")
        print(df_insert)
        if df_insert['install_location'][0] == "Inlet":
            rows = list(
                df_insert[[
                    "data_type" ,
                    "asset_number" ,
                    "asset_name" ,
                    "system" ,
                    "install_location" ,
                    "device_type" ,
                    "device_id" ,
                    "project" ,
                    "report_time" ,           
                    "timestamp" ,         
                    "sensor_type" ,
                    "operation" ,
                    "value_raw" ,
                    "value" ,
                    "clean_air_damper_open_alarm" ,
                    "co2_level_enable_scrub_mode" ,
                    "co2_level_scrub_mode" ,
                    "exhaust_air_damper_open_alarm" ,
                    "fan_alarm" ,
                    "fan_speed" ,
                    "fire_alarm" ,
                    "high_temperature_alarm" ,
                    "hlr_connect_status" ,
                    "hlr_operation_mode" ,
                    "interlock_status" ,
                    "km1_no_feedback_alarm" ,
                    "service_door_alarm" ,
                    "switch_co2_state" ,
                    "switch_interlock_state" ,
                    "temp_before_filter" 
                ]].itertuples(index=False, name=None)
            )
            # print("Here")
            # print("rows =>", rows)
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.executemany("""
                    INSERT INTO sensor_data (
                        data_type,
                        asset_number,
                        asset_name,
                        system,
                        install_location,
                        device_type,
                        device_id,
                        project,
                        report_time,
                        timestamp,
                        sensor_type,
                        operation,
                        value_raw,
                        value,
                        clean_air_damper_open_alarm,
                        co2_level_enable_scrub_mode,
                        co2_level_scrub_mode,
                        exhaust_air_damper_open_alarm,
                        fan_alarm,
                        fan_speed,
                        fire_alarm,
                        high_temperature_alarm,
                        hlr_connect_status,
                        hlr_operation_mode,
                        interlock_status,
                        km1_no_feedback_alarm,
                        service_door_alarm,
                        switch_co2_state,
                        switch_interlock_state,
                        temp_before_filter
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, rows)
                conn.commit()
            return jsonify({
                "ok": True,
                "received_rows": len(merged),
                "prepared_rows": before,
                "inserted_rows": after,
                "skipped_rows": before - after
            }), 200
        else:
            rows = list(
                df_insert[[
                    "data_type" ,
                    "asset_number" ,
                    "asset_name" ,
                    "system" ,
                    "install_location" ,
                    "device_type" ,
                    "device_id" ,
                    "project" ,
                    "report_time" ,           
                    "timestamp" ,         
                    "sensor_type" ,
                    "operation" ,
                    "value_raw" ,
                    "value" 
                ]].itertuples(index=False, name=None)
            )
            # print("Here")
            # print("rows =>", rows)
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.executemany("""
                    INSERT INTO sensor_data (
                        data_type,
                        asset_number,
                        asset_name,
                        system,
                        install_location,
                        device_type,
                        device_id,
                        project,
                        report_time,
                        timestamp,
                        sensor_type,
                        operation,
                        value_raw,
                        value
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?
                    )
                """, rows)
                conn.commit()
            return jsonify({
                "ok": True,
                "received_rows": len(merged),
                "prepared_rows": before,
                "inserted_rows": after,
                "skipped_rows": before - after
            }), 200

        # return  "ok", 200

    # except Exception as e:
    #     print(f"error => {e}")
    #     return jsonify({"ok": False, "error": str(e)}), 500

#  //  http://127.0.0.1:3012/get?project=d17&start=1760018660000&end=1761782581000
@app.route("/backend_c/get", methods=["GET"])
def RouteGet():
    try:
        # sensor_type = request.args.get("sensor_type") ## all
        # asset_name = request.args.get("asset_name") ## all
        project = request.args.get("project")
        startDate = request.args.get('start')
        endDate = request.args.get('end')

        # print(startDate, endDate, sensor_type, asset_name ,project)

        
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("""
                    SELECT DISTINCT 
                            data_type,
                            asset_number,
                            asset_name,
                            system,
                            install_location,
                            device_type,
                            device_id,
                            project,
                            hlr_operation_mode,
                            timestamp,
                            sensor_type,
                            value
                            FROM sensor_data
                            WHERE project = ?
                            AND timestamp BETWEEN ? AND ? 
                            ORDER BY timestamp ASC
            """, (project, int(startDate), int(endDate)))  
        rows = cur.fetchall()
        return jsonify({"ok": True, "rows": rows}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    
# @app.route("/backend_c/get/compare/co2", methods=["GET"])

# hlr operation mode
@app.route("/backend_c/get/param", methods=["GET"])
def RoutGetParam():
    try:
        data_type = []
        asset_number = []
        asset_name = []
        system = []
        install_location=[]
        device_type =[]
        device_id=[]
        project=[]
        sensor_type=[]
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT 
                        data_type, 
                        asset_number,
                        asset_name,
                        system,
                        install_location,
                        device_type,
                        device_id,
                        project,
                        sensor_type
                        FROM sensor_data

            """)  
            rows = cur.fetchall()
            
            for el in rows:
                data_type.append(el[0])
                asset_number.append(el[1])
                asset_name.append(el[2])
                system.append(el[3])
                install_location.append(el[4])
                device_type.append(el[5])
                device_id.append(el[6])
                project.append(el[7])
                sensor_type.append(el[8])

            payload_param = {
                "data_type":list(set(data_type)),
                "asset_number": list(set(asset_number)),
                "asset_name": list(set(asset_name)),
                "system":list(set(system)),
                "install_location": list(set(install_location)),
                "device_type": list(set(device_type)),
                "device_id": list(set(device_id)),
                "project": list(set(project)),
                "sensor_type": list(set(sensor_type))
            }
            # print(payload_param)
            return {"ok": True, "data":payload_param}, 200
    except Exception as e:
        # print(f"RoutGetParam  error => {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3012, debug=True)

 