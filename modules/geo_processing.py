""" Functions used for geodata processing """
import geopy.distance

VALID_POINT_DISTANCE_IN_METERS = 1000
KNOWN_LOCATIONS = {
    "50.76;-1.29": "Cowes East",
    "50.76;-1.30": "Cowes",
    "50.83;-0.96": "Hayling Island (Havant)",
    "50.80;-0.94": "Hayling Island"
}


def calc_distance_between_coordinates(
    current_coord,
    prev_coord
):
    """ Returns distane between two coordinates in meters """
    return geopy.distance.geodesic(current_coord, prev_coord).m


def check_if_coordinate_is_valid(
    current_point,
    prev_point,
    threshold=VALID_POINT_DISTANCE_IN_METERS
):
    """
    Checks if point in current given track is valid
    """

    # Check if speed is invalid of if the boat is on land (speed = 0)
    if current_point["speed"] <= 0:
        return False

    # Check if course is invalid
    elif current_point["course"] <= 0:
        return False

    # Check if distance is realistic (detect outliers)
    elif prev_point and calc_distance_between_coordinates(
        (current_point["latitude"], current_point["longitude"]),
        (prev_point["latitude"], prev_point["longitude"])
    ) > threshold:
        return False

    return True


def find_location_tag(latitude, longitude):
    """ Finds location tag based on current lat / lon"""

    shortened_lat = str(latitude)[:5]
    shortened_lon = str(longitude)[:5]

    # TODO: Replace with dataset that has more coordinates in the future
    return KNOWN_LOCATIONS.get(f"{shortened_lat};{shortened_lon}", "")
