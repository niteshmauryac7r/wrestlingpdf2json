import os
import re
import camelot
from pdfminer.high_level import extract_text
import requests
from flask import Flask, request, jsonify
from azure.storage.blob import BlobServiceClient, BlobClient
import tempfile

# Initialize Flask
app = Flask(__name__)

# Initialize the Azure Blob Storage client
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=blobpdf2json;AccountKey=lk/UB30zmCefXd0ewV+dXvteu0G/qFn2TCnTPvo89ll0CRe8EGim883H9lEVXxVA5ErqfXqXoK5B+AStQm8DxA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_name = "wrestlingpdf2json"

def fetch_participant_details(target_player_detail_id):
    api_url = f"http://216.48.180.88:8099/api/tsr/participantslist?token=37|XVJoJ36iTyTqSpucMrDEFjZWpJCiqn81PIbOgb1Q&sport=wrestling"

    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()

        if "participants_list" in data:
            participants = data["participants_list"]

            selected_participant = None
            for participant in participants:
                if participant["player_detail_id"] == target_player_detail_id:
                    selected_participant = participant
                    break

            if selected_participant:
                selected_participant_details = {
                    "Player ID": selected_participant['player_detail_id'],
                    "Kitd Unique ID": selected_participant['kitd_unique_id'],
                    "player_name": f"{selected_participant['first_name']} {selected_participant['last_name']}",
                    "Date of Birth": selected_participant['date_of_birth'],
                    "Gender": selected_participant['gender'],
                    "State": f"{selected_participant['state_name']} ",
                    "State_id":f"{selected_participant['state_id']}",
                    "Sport": f"{selected_participant['sport_name']}",
                    "Sport_ID": f"{selected_participant['sport_id']}",
                    "Category": f"{selected_participant['category_name']} ",
                    "Category_ID":f"{selected_participant['category_id']}",
                    "Event": f"{selected_participant['event_name']}",
                    "Event_ID": f"{selected_participant['event_id']}"
                }
                return selected_participant_details
            else:
                print(f"No participant found with player_detail_id {target_player_detail_id}")
        else:
            print("No 'participants_list' key found in the response.")
    else:
        print(f"Request failed with status code: {response.status_code}")


def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return 0

def process_pdf(pdf_file_path, selected_participant_details, selected_participants_details2):
    try:
        # Extract text from the PDF
        text = extract_text(pdf_file_path)
        # Initialize a dictionary to store extracted data
        data = {}

        # Use regular expressions to search for the desired data
        classification_points_match = re.search(r'CLASSIFICATION POINTS\n\n(\d+)\n\n(\d+)', text)

        # Check if the match was found
        if classification_points_match:
            # Extract the matched data
            data["point1"] = classification_points_match.group(1)
            data["point2"] = classification_points_match.group(2)
            point1 = int(data["point1"])
            point2 = int(data["point2"])
        # Use regular expressions to match and extract data
        match_number = re.search(r'\d+\s+Match\s+number', text)
        if match_number:
            data["match_number"] = match_number.group(0).split()[0]

        tournament_name = re.search(r'Khelo MP Youth Games \d{4} - [A-Za-z]{3} \d{4} - [A-Za-z]+', text)
        if tournament_name:
            data["tournament_name"] = tournament_name.group(0)

            # Split tournament_name into components
            components = tournament_name.group(0).split('-')
            sport = components[0].strip()
            date = components[1].strip()
            event = components[2].strip()

        match_info = re.search(r'U\d+ - [A-Z]+ - \d+ kg / \d/\d Final', text)
        if match_info:
            data["match_info"] = match_info.group(0)

            # Split match_info into components
            components1 = match_info.group(0).split('-')
            category = components1[0].strip()
            Weight_group = components1[2].strip()

            components2 = Weight_group.split('/ ')
            wg = components2[0].strip()
            stage = components2[1].strip()

        # Read the table using Camelot
        tables = camelot.read_pdf(pdf_file_path)
        a = tables[0].df

        # Extract athlete and state information
        athlete1 = a.iloc[2, 0].split('\n')[0]
        state1 = a.iloc[2, 0].split('\n')[1]

        athlete2 = a.iloc[2, 4].split('\n')[0]
        state2 = a.iloc[2, 4].split('\n')[1]

        total_athlete1 = safe_int(a.iloc[4,2]) + safe_int(a.iloc[6,2])
        total_athlete2 = safe_int(a.iloc[4,6]) + safe_int(a.iloc[6,6])

        if total_athlete1 > total_athlete2:
            winner = athlete1
            losser = athlete2
        else:
            winner = athlete2
            losser = athlete1

        # Create the JSON data structure
        data1 = {
            9: {
                "status": "success",
                "statusCode": 200,
                "message": "Ok",
                "data": {
                    "tournament_name": "National Games 2023-Goa",
                    "sport": "Wrestling",
                    "sport_id": "2",
                    "match_id": 7,
                    "event": "Wrestling 51 Kg",
                    "event_id": 1042,
                    "schedule_date_and_time": date,
                    "venue": "S.P.M.Stadium, GoaCollage,University Farmagudi Ponda Panjim",
                    "stage": "Qualifying Round",
                    "team1": "Madhya Pradesh",
                    "team2": "Andhra Pradesh",
                    "won": "Madhya Pradesh",
                    "loss": "Andhra Pradesh",
                    "team1_total_score": 10,
                    "team2_total_score": 3,
                    "sub_matches_summary": {
                        "team1_player_details": selected_participant_details,
                        "team2_player_details": selected_participant_details2,
                        "individual_matches_score": {
                            "team1_score": [4,6],
                            "team2_score": [2,1],
                        },
                        "team1_score": 10,
                        "team2_score": 3,
                        "won": "Madhya Pradesh",
                        "loss": "Andhra Pradesh",
                        "Classification_points": f"{point1}:{point2}"
                    }
                }
            }
        }

        return data1

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


# Fetch the selected participant's details for player_id1
target_player_detail_id1 = 60689
selected_participant_details1 = fetch_participant_details(target_player_detail_id1)

# Fetch the selected participant's details for player_id2
target_player_detail_id2 = 191512
selected_participant_details2 = fetch_participant_details(target_player_detail_id2)


@app.route('/upload', methods=['POST'])
def upload_pdf_to_blob():
    try:
        # Check if the POST request contains a file
        if 'file' not in request.files:
            return jsonify({"error": "No file part"}), 400

        pdf_file = request.files['file']

        # Check if the file has an allowed extension (e.g., PDF)
        if pdf_file.filename == '':
            return jsonify({"error": "No selected file"}), 400
        if not pdf_file.filename.endswith('.pdf'):
            return jsonify({"error": "Invalid file format"}), 400

        # Create a connection to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_name = "wrestlingpdf2json"

        # Upload the PDF file to Azure Blob Storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=pdf_file.filename)
        blob_client.upload_blob(pdf_file)

        # Process the uploaded PDF from Azure Blob Storage and update combined results
        pdf_data = process_pdf(pdf_file.filename, selected_participant_details1, selected_participant_details2)

        return jsonify("Success"), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/process_blob', methods=['GET'])
def process_blob():
    try:
        # Get the name of the blob from the request parameter
        blob_name = request.args.get('blob_name')

        # Download the blob from Azure Blob Storage to a temporary file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        temp_file_path = os.path.join(tempfile.mkdtemp(), blob_name)
        with open(temp_file_path, "wb") as file:
            file.write(blob_client.download_blob().readall())

        # Process the downloaded PDF file and update combined results
        pdf_data = process_pdf(temp_file_path, selected_participant_details1, selected_participant_details2)

        return jsonify(pdf_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True,port=8080)
