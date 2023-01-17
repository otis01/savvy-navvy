""" Functions for uploading / downloading files """
import copy
import requests
import os
from pathlib import Path
import glob
import zipfile
import json
from shapely import geometry, wkb
import xmltodict
from modules.geo_processing import check_if_coordinate_is_valid, find_location_tag
import shutil
import boto3

root_dir = Path(os.path.realpath(__file__)).parent.parent
download_dir = os.path.join(root_dir, "downloads")
upload_dir = os.path.join(root_dir, "uploads")

ZIP_FILE_URL = "https://drive.google.com/u/0/uc?id=1WsHCvnJPJkRYu-g83FDFd5rYqDUyNICY&export=download"
S3_BUCKET = "my-test-bucket"
S3_KEY = "geo-processing"


def download_zip_file():
    """ Downloads and unpacks zip file"""

    with open(f"{download_dir}/data.zip", "wb+") as f:

        r = requests.get(ZIP_FILE_URL)
        r.raise_for_status()

        for line in r.iter_content(chunk_size=8192):
            f.write(line)

    zipfile.ZipFile(f"{download_dir}/data.zip").extractall(download_dir)


def upload_to_s3():

    s3_client = boto3.client("s3")
    to_upload_files = [
        (file, f"{upload_dir}/{file}")
        for file in os.listdir(upload_dir)
    ]
    for file_name, file_location in to_upload_files:
        s3_client.upload_fileobj(file_location, Bucket=S3_BUCKET, Key=f"{S3_KEY}/{file_name}")


def delete_file(file_path):

    try:
        os.remove(file_path)
    except OSError as e:
        print("Error: %s : %s" % (file_path, e.strerror))


def delete_files():

    for file_path in glob.glob(f'{download_dir}/!(*.gitkeep)', recursive=True):
        delete_file(file_path)

    for file_path in glob.glob(f'{upload_dir}/!(*.gitkeep)', recursive=True):
        delete_file(file_path)


def create_geojson(track_name, point_id, point_data):
    """ Creates GeoJSON object from point data"""

    return {
        "track_name": track_name,
        "id": point_id,
        "latitude": float(point_data["@lat"]),
        "longitude": float(point_data["@lon"]),
        "coordinates": wkb.dumps(
            geometry.Point(
                float(point_data["@lat"]),
                float(point_data["@lon"])
            ),
            hex=True
        ),
        "gpx_time": point_data["time"].replace("T", " "),
        "speed": float(point_data["extensions"]["gpxtpx:TrackPointExtension"]["gpxtpx:speed"]),
        "course": float(point_data["extensions"]["gpxtpx:TrackPointExtension"]["gpxtpx:course"])
    }


def process_geodata():
    """ Read local files from unzipped directory """

    manifest_dict = {
      "entries": []
    }

    gpx_files = [
        (file.rstrip(".gpx"), f"{download_dir}/{file}")
        for file in os.listdir(download_dir) if file.endswith(".gpx")
    ]

    for track_name, file_path in gpx_files:

        # Read GPX file
        with open(file_path, "r") as gpx_file:
            data_dict = xmltodict.parse(gpx_file.read())

        # Convert to CSV file
        with open(f"{upload_dir}/{track_name}.csv", "w+") as csv_file:

            # Read data
            gpx_track_data = data_dict["gpx"]["trk"]["trkseg"]["trkpt"]
            prev_track_point = None

            for point_id, point_data in enumerate(gpx_track_data):

                # Create GeoJSON
                geojson = create_geojson(track_name, point_id, point_data)

                # Add Location Tag
                geojson["location_tag"] = find_location_tag(
                    geojson["latitude"],
                    geojson["longitude"]
                )

                # Check if GeoJSON is valid
                is_valid = check_if_coordinate_is_valid(geojson, prev_track_point)
                geojson["is_valid"] = 1 if is_valid else 0
                prev_track_point = copy.deepcopy(geojson)

                # Write headers if first row
                if point_id == 0:
                    csv_file.write(f'{"|".join(geojson.keys())}\n')

                # Write out data
                csv_file.write(f'{"|".join([str(x) for x in geojson.values()])}\n')

            # Update manifest file
            manifest_dict["entries"].append(
                {"url": f"s3://my-test-bucket/geo-processing/{track_name}.csv", "mandatory": True}
            )

    with open(f"{upload_dir}/manifest.json", "w") as manifest_file:
        json.dump(manifest_dict, manifest_file)
