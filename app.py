# app.py
import os, io, math, datetime, time
import pandas as pd
from datetime import datetime as dt, timedelta
from flask import Flask, request, send_file, jsonify
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from pyairtable import Table

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

def get_airtable_table():
    """Get Airtable table instance using environment variables."""
    api_key = os.environ.get("AIRTABLE_API_KEY")
    base_id = os.environ.get("AIRTABLE_BASE_ID")
    table_name = os.environ.get("AIRTABLE_TABLE_NAME", "Last Locations")
    
    if not api_key or not base_id:
        return None  # Airtable not configured, will fall back to old logic
    
    return Table(api_key, base_id, table_name)

def get_last_positions_from_airtable():
    """Read last reported positions from Airtable. Returns dict: {serial: {'lat': float, 'lon': float}}"""
    table = get_airtable_table()
    if not table:
        return {}
    
    try:
        records = table.all()
        positions = {}
        for record in records:
            fields = record.get('fields', {})
            serial = fields.get('Serial')
            if serial:
                positions[serial] = {
                    'lat': fields.get('Last_Report_Lat'),
                    'lon': fields.get('Last_Report_Lon')
                }
        return positions
    except Exception as e:
        print(f"Error reading from Airtable: {e}")
        return {}

def update_airtable_positions(current_positions):
    """Update Airtable with current positions. Handles rate limiting for 133+ trackers."""
    table = get_airtable_table()
    if not table:
        return
    
    try:
        # Get all existing records to find record IDs (1 request)
        all_records = table.all()
        record_map = {}  # {serial: record_id}
        for record in all_records:
            serial = record.get('fields', {}).get('Serial')
            if serial:
                record_map[serial] = record['id']
        
        # Separate updates and creates
        updates_to_do = []
        creates_to_do = []
        
        for serial, pos in current_positions.items():
            fields = {
                'Serial': serial,
                'Last_Report_Lat': pos['lat'],
                'Last_Report_Lon': pos['lon']
            }
            
            if serial in record_map:
                updates_to_do.append((record_map[serial], fields))
            else:
                creates_to_do.append(fields)
        
        # Process updates with rate limiting (max ~5 requests/sec to stay under 5/sec limit)
        for idx, (record_id, fields) in enumerate(updates_to_do):
            try:
                table.update(record_id, fields)
                # Wait 0.2 seconds between requests (~5 requests/sec = safe margin)
                if idx < len(updates_to_do) - 1:  # Don't wait after last update
                    time.sleep(0.2)
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "Rate limit" in error_str or "429" in str(getattr(e, 'status_code', '')):
                    print(f"Rate limit hit during update, waiting 30 seconds...")
                    time.sleep(30)
                    # Retry once
                    try:
                        table.update(record_id, fields)
                    except Exception as retry_e:
                        print(f"Error retrying update for record {record_id}: {retry_e}")
                else:
                    print(f"Error updating record {record_id}: {e}")
        
        # Process creates with rate limiting
        for idx, fields in enumerate(creates_to_do):
            try:
                table.create(fields)
                # Wait 0.2 seconds between requests
                if idx < len(creates_to_do) - 1:  # Don't wait after last create
                    time.sleep(0.2)
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "Rate limit" in error_str or "429" in str(getattr(e, 'status_code', '')):
                    print(f"Rate limit hit during create, waiting 30 seconds...")
                    time.sleep(30)
                    # Retry once
                    try:
                        table.create(fields)
                    except Exception as retry_e:
                        print(f"Error retrying create: {retry_e}")
                else:
                    print(f"Error creating record: {e}")
                    
    except Exception as e:
        print(f"Error updating Airtable: {e}")

def calculate_duration(data):
    data['Timestamp'] = pd.to_datetime(data['Timestamp']).dt.tz_localize(None)
    data.sort_values(by='Timestamp', inplace=True)
    data['Location_Changed'] = (data['Address'] != data['Address'].shift(1))
    data['Location_Start'] = data['Timestamp'].where(data['Location_Changed']).ffill()
    most_recent_row = data.iloc[-1]
    duration = most_recent_row['Timestamp'] - most_recent_row['Location_Start']
    formatted_duration = f"{duration.days} days {str(duration).split(' ')[-1]}"
    return formatted_duration

def check_moved_in_24hr_vs_airtable(current_lat, current_lon, serial, airtable_positions, threshold=300):
    """
    Compare current tracker position (from data CSV) to last position stored in Airtable.
    Returns "Y" if moved > threshold meters, else "N".
    """
    if pd.isna(current_lat) or pd.isna(current_lon):
        return "N"
    
    if not serial or serial not in airtable_positions:
        return "N"  # No previous position in Airtable
    
    # Get last reported position from Airtable
    last_pos = airtable_positions[serial]
    last_lat = last_pos.get('lat')
    last_lon = last_pos.get('lon')
    
    if pd.isna(last_lat) or pd.isna(last_lon) or last_lat is None or last_lon is None:
        return "N"
    
    # Convert to float (Airtable values might be strings)
    try:
        last_lat = float(last_lat)
        last_lon = float(last_lon)
        current_lat = float(current_lat)
        current_lon = float(current_lon)
    except (ValueError, TypeError):
        return "N"  # Invalid numeric values
    
    # Calculate distance
    dist = haversine(last_lat, last_lon, current_lat, current_lon)
    return "Y" if dist > threshold else "N"

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
    if 'Lat' not in main_data.columns or 'Lon' not in main_data.columns:
        raise ValueError("Data CSV missing required columns: Lat and/or Lon")

    # Read last positions from Airtable
    airtable_positions = get_last_positions_from_airtable()
    
    # Calculate durations from location_data (still needs history for Time_At_Location)
    durations = {}
    for serial, group in location_data.groupby('Serial'):
        durations[serial] = calculate_duration(group.copy())
    
    # Compare positions from main_data (data CSV) to Airtable
    moved_flags = {}
    current_positions = {}  # Track current positions for Airtable update
    
    # Process each row in main_data (the data CSV with final positions)
    for idx, row in main_data.iterrows():
        serial = row.get('Serial')
        if not serial:
            continue
            
        # Get current position from main_data (the data CSV - last known location)
        current_lat = pd.to_numeric(row.get('Lat'), errors='coerce')
        current_lon = pd.to_numeric(row.get('Lon'), errors='coerce')
        
        # Compare to Airtable position
        moved_flags[serial] = check_moved_in_24hr_vs_airtable(
            current_lat, current_lon, serial, airtable_positions
        )
        
        # Store current position for Airtable update (from main_data)
        if not pd.isna(current_lat) and not pd.isna(current_lon):
            current_positions[serial] = {
                'lat': float(current_lat),
                'lon': float(current_lon)
            }

    # add outputs
    main_data['Time_At_Location'] = main_data['Serial'].map(lambda x: durations.get(x, "No data"))
    main_data['Moved in 24hr'] = main_data['Serial'].map(lambda x: moved_flags.get(x, "N"))
    if 'Lat' in main_data.columns and 'Lon' in main_data.columns:
        main_data['Google Maps Link'] = main_data.apply(
            lambda row: create_google_maps_link(row['Lat'], row['Lon']), axis=1
        )

    # write to Excel in memory
    buf = io.BytesIO()
    main_data.to_excel(buf, index=False)
    buf.seek(0)
    buf = add_filters_to_excel(buf)

    # Update Airtable with current positions from main_data (data CSV)
    update_airtable_positions(current_positions)

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
    try:
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

    except Exception as e:
        import traceback
        import sys
        error_msg = f"Error in /process: {str(e)}"
        print(error_msg, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5050)), debug=True)
