import json
import math
import copy
import os

# Function to calculate distance between two coordinates (in km) using Haversine formula
def haversine_distance(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

# Function to check if a point is within geographical bounds
def is_within_bounds(lat, lng, bounds):
    return (bounds['south'] <= lat <= bounds['north'] and
            bounds['west'] <= lng <= bounds['east'])

# Function to find the country for a given coordinate
def find_country(lat, lng, gps_db):
    for country_code, country_data in gps_db['countries'].items():
        if is_within_bounds(lat, lng, country_data['bounds']):
            return country_code, country_data
    return None, None

# Function to find subdivision (state/province/region) within a country
def find_subdivision(lat, lng, country_data):
    # Different countries have different subdivision types
    subdivision_types = [
        'states', 'provinces', 'regions', 'emirates', 'countries', 'governorates'  # Added governorates for Egypt
    ]
    
    for subdivision_type in subdivision_types:
        if subdivision_type in country_data:
            for sub_code, sub_data in country_data[subdivision_type].items():
                if is_within_bounds(lat, lng, sub_data['bounds']):
                    return sub_code, sub_data, subdivision_type
    
    return None, None, None

# Function to find city based on proximity to major cities
def find_city(lat, lng, subdivision_data, max_distance_km=50):
    closest_city = None
    closest_distance = float('inf')
    
    if 'major_cities' in subdivision_data:
        for city_name, city_coords in subdivision_data['major_cities'].items():
            distance = haversine_distance(lat, lng, city_coords['lat'], city_coords['lng'])
            if distance < closest_distance:
                closest_distance = distance
                closest_city = city_name
    
    # Only return the city if it's within the maximum distance
    if closest_distance <= max_distance_km:
        return closest_city, closest_distance
    return None, None

# Function to find landmarks near coordinates
def find_landmark(lat, lng, subdivision_data, max_distance_km=5):
    if 'landmarks' not in subdivision_data:
        return None, None, None
        
    closest_landmark = None
    closest_distance = float('inf')
    landmark_info = None
    
    for landmark_name, landmark_data in subdivision_data['landmarks'].items():
        distance = haversine_distance(lat, lng, landmark_data['lat'], landmark_data['lng'])
        if distance < closest_distance and distance <= max_distance_km:
            closest_distance = distance
            closest_landmark = landmark_name
            landmark_info = landmark_data
    
    return closest_landmark, landmark_info, closest_distance

# Function to find county based on coordinates (for US locations)
def find_county(lat, lng, subdivision_data):
    # If we have a landmark with county info, use that
    if 'landmarks' in subdivision_data:
        for landmark_name, landmark_data in subdivision_data['landmarks'].items():
            if 'county' in landmark_data:
                # Check if the point is very close to the landmark (1km)
                distance = haversine_distance(lat, lng, landmark_data['lat'], landmark_data['lng'])
                if distance <= 1:
                    return landmark_data['county']
    
    # For future expansion: could add county boundaries
    return None

# Main function to determine location details from coordinates
def determine_location(lat, lng, gps_db):
    result = {
        "city": None,
        "state": None,
        "country": None,
        "county": None,
        "landmark": None,
        "postal_code": None,
        "full_location": None,
        "approximate": True  # Flag to indicate this is an approximate match
    }
    
    # Find country
    country_code, country_data = find_country(lat, lng, gps_db)
    if country_data:
        result["country"] = country_data["name"]
        
        # Find subdivision (state/province/region)
        sub_code, sub_data, sub_type = find_subdivision(lat, lng, country_data)
        if sub_data:
            # Handle special case for UK where the subdivisions are countries
            if sub_type == 'countries':
                # For UK, we still store England, Scotland, etc. in the state field
                result["state"] = sub_data["name"]
            else:
                result["state"] = sub_data["name"]
            
            # Check for landmarks first (more precise)
            landmark_name, landmark_info, landmark_distance = find_landmark(lat, lng, sub_data)
            if landmark_name:
                result["landmark"] = landmark_name
                # If landmark has associated city info, use that
                if landmark_info and 'city' in landmark_info:
                    result["city"] = landmark_info["city"]
                if landmark_info and 'county' in landmark_info:
                    result["county"] = landmark_info["county"]
            
            # If no landmark city, find closest city
            if not result["city"]:
                city_name, city_distance = find_city(lat, lng, sub_data)
                if city_name:
                    result["city"] = city_name
            
            # Find county for US locations
            if country_code == "USA" and not result["county"]:
                county = find_county(lat, lng, sub_data)
                if county:
                    result["county"] = county
        
        # Construct full_location with landmark if available
        location_parts = []
        if result["landmark"]:
            location_parts.append(result["landmark"])
        if result["city"] and (not result["landmark"] or result["landmark"] != result["city"]):
            location_parts.append(result["city"])
        if result["county"] and country_code == "USA":
            location_parts.append(f"{result['county']} County")
        if result["state"]:
            location_parts.append(result["state"])
        if result["country"]:
            location_parts.append(result["country"])
        
        if location_parts:
            result["full_location"] = ", ".join(location_parts)
        else:
            result["full_location"] = result["country"]
    
    return result

# Process JSON data and update location information
def process_instagram_data(input_file, output_file, gps_db_file):
    # Load GPS database
    with open(gps_db_file, 'r', encoding='utf-8') as f:
        gps_db = json.load(f)
    
    # Load Instagram data
    with open(input_file, 'r', encoding='utf-8') as f:
        instagram_data = json.load(f)
    
    # Create a deep copy to avoid modifying the original data while iterating
    updated_data = copy.deepcopy(instagram_data)
    
    # Process each user in the data
    for user in updated_data:
        if 'location_analysis' in user and 'home_location' in user['location_analysis']:
            home_location = user['location_analysis']['home_location']
            
            # Only process if home_location exists and coordinates are available
            if home_location and 'coordinates' in home_location and home_location['coordinates']:
                lat = home_location['coordinates']['lat']
                lng = home_location['coordinates']['lng']
                
                # Get correct location data based on coordinates
                location_data = determine_location(lat, lng, gps_db)
                
                # Update only the location fields, preserving other data
                home_location['city'] = location_data['city']
                home_location['state'] = location_data['state']
                home_location['country'] = location_data['country']
                
                # Add landmark information if available
                if 'landmark' in location_data and location_data['landmark']:
                    if 'landmark' not in home_location:
                        home_location['landmark'] = None
                    home_location['landmark'] = location_data['landmark']
                
                # Add county information for US locations
                if 'county' in location_data and location_data['county']:
                    if 'county' not in home_location:
                        home_location['county'] = None
                    home_location['county'] = location_data['county']
                
                # Update full_location if we have better data
                if location_data['full_location']:
                    home_location['full_location'] = location_data['full_location']
    
    # Save updated data
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(updated_data, f, indent=2, ensure_ascii=False)
    
    return len(updated_data)

if __name__ == "__main__":
    # Set file paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, "master_analyzed.json")
    output_file = os.path.join(script_dir, "insta_updated_with_location.json")
    gps_db_file = os.path.join(script_dir, "gps_location_database.json")
    
    # Process the data
    user_count = process_instagram_data(input_file, output_file, gps_db_file)
    print(f"Processing complete. Updated location data for {user_count} users.")
    print(f"Updated data saved to: {output_file}")
