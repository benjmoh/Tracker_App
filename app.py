# app.py
import os, io, math, datetime
import pandas as pd
from datetime import datetime as dt, timedelta
from flask import Flask, request, send_file, jsonify
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

app = Flask(__name__)

def create_google_maps_link(lat, lon):
    if (pd.isna(lat) or pd.isna(lon)) or (lat == 0 and lon == 0):
        return "NULL"
    return f'=HYPERLINK("https://www.google.com/maps?q={lat},{lon}", "View Location")'

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1 = math.radians(lat1); phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def calculate_duration(data):
    data['Timestamp'] = pd.to_datetime(data['Timestamp']).dt.tz_localize(None)
    data.sort_values(by='Timestamp', inplace=True)
    data['Location_Changed'] = (data['Address'] != data['Address'].shift(1))
    data['Location_Start'] = data['Timestamp'].where(data['Location_Changed']).ffill()
    most_recent_row = data.iloc[-1]
    duration = most_recent_row['Timestamp'] - most_recent_row['Location_Start']
    formatted_duration = f"{duration.days} days {str(duration).split(' ')[-1]}"
    return formatted_duration

def check_moved_in_48hrs(data, threshold=50):
    data['Timestamp'] = pd.to_datetime(data['Timestamp']).dt.tz_localize(None)
    data = data.sort_values(by='Timestamp')
    
    # Convert Lat and Lon to numeric (they might be strings from CSV)
    data['Lat'] = pd.to_numeric(data['Lat'], errors='coerce')
    data['Lon'] = pd.to_numeric(data['Lon'], errors='coerce')
    
    cutoff_time = dt.now() - timedelta(hours=48)
    recent_data = data[data['Timestamp'] >= cutoff_time]
    before_cutoff = data[data['Timestamp'] < cutoff_time]
    
    # Check movement between consecutive points within the 48hr window
    if len(recent_data) >= 2:
        for i in range(1, len(recent_data)):
            lat1 = recent_data.iloc[i-1]['Lat']
            lon1 = recent_data.iloc[i-1]['Lon']
            lat2 = recent_data.iloc[i]['Lat']
            lon2 = recent_data.iloc[i]['Lon']
            
            if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
                continue
                
            dist = haversine(lat1, lon1, lat2, lon2)
            if dist > threshold:
                return "Y"
    
    # BUG FIX: Check if there was movement FROM before cutoff TO within cutoff
    # This catches cases where tracker moved at the start of the 48hr window
    if len(before_cutoff) > 0 and len(recent_data) > 0:
        last_before = before_cutoff.iloc[-1]
        first_recent = recent_data.iloc[0]
        
        lat1 = last_before['Lat']
        lon1 = last_before['Lon']
        lat2 = first_recent['Lat']
        lon2 = first_recent['Lon']
        
        if not (pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2)):
            dist = haversine(lat1, lon1, lat2, lon2)
            if dist > threshold:
                return "Y"
    
    return "N"

def add_filters_to_excel(file_like_bytesio):
    file_like_bytesio.seek(0)
    wb = load_workbook(file_like_bytesio)
    ws = wb.active
    max_col = ws.max_column
    max_row = ws.max_row
    ws.auto_filter.ref = f"A1:{get_column_letter(max_col)}{max_row}"
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out

def process_dataframes(location_data: pd.DataFrame, main_data: pd.DataFrame, date_for_name=None):
    # validate required columns
    for col in ['Serial', 'Address', 'Timestamp', 'Lat', 'Lon']:
        if col not in location_data.columns:
            raise ValueError(f"Location CSV missing required column: {col}")
    if 'Serial' not in main_data.columns:
        raise ValueError("Data CSV missing required column: Serial")

    durations = {}
    moved_flags = {}
    for serial, group in location_data.groupby('Serial'):
        durations[serial] = calculate_duration(group.copy())
        moved_flags[serial] = check_moved_in_48hrs(group.copy())

    # add outputs
    main_data['Time_At_Location'] = main_data['Serial'].map(lambda x: durations.get(x, "No data"))
    main_data['Moved > 50m in 48hr'] = main_data['Serial'].map(lambda x: moved_flags.get(x, "N"))
    if 'Lat' in main_data.columns and 'Lon' in main_data.columns:
        main_data['Google Maps Link'] = main_data.apply(
            lambda row: create_google_maps_link(row['Lat'], row['Lon']), axis=1
        )

    # write to Excel in memory
    buf = io.BytesIO()
    main_data.to_excel(buf, index=False)
    buf.seek(0)
    buf = add_filters_to_excel(buf)

    # name
    if not date_for_name:
        today = datetime.datetime.now()
        date_for_name = f"{today.year}_{today.month:02d}_{today.day:02d}"
    filename = f"data_{date_for_name}_updated.xlsx"
    return filename, buf

@app.route("/process", methods=["POST"])
def process():
    """
    Accepts:
      - multipart/form-data with files:
           location_csv (file), data_csv (file)
        OR JSON with:
           location_url, data_url (public URLs Zapier can provide)
      - Optional: date_for_name (string "YYYY_MM_DD") for the output filename.
    Returns: Excel file as an attachment (application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)
    """
    date_for_name = request.form.get("date_for_name")

    # Case 1: files uploaded directly
    if "location_csv" in request.files and "data_csv" in request.files:
        location_data = pd.read_csv(request.files["location_csv"])
        main_data = pd.read_csv(request.files["data_csv"])
        fname, buf = process_dataframes(location_data, main_data, date_for_name)
        return send_file(buf, as_attachment=True, download_name=fname,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Case 2: URLs provided (Zapier can pass Google Drive direct download links or other public URLs)
    payload = request.get_json(silent=True) or {}

    if not date_for_name:
        date_for_name = payload.get("date_for_name")

    if payload.get("location_url") and payload.get("data_url"):
        # Note: Render blocks outbound to Google Drive preview URLs unless they're direct-download.
        # Prefer supplying direct file content via multipart where possible.
        import requests
        loc = requests.get(payload["location_url"])
        dat = requests.get(payload["data_url"])
        loc.raise_for_status(); dat.raise_for_status()
        location_data = pd.read_csv(io.BytesIO(loc.content))
        main_data = pd.read_csv(io.BytesIO(dat.content))
        fname, buf = process_dataframes(location_data, main_data, date_for_name)
        return send_file(buf, as_attachment=True, download_name=fname,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return jsonify({
        "error": "Provide location_csv & data_csv files, or location_url & data_url in the request body."
    }), 400

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5050)), debug=True)
