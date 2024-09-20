from connection import call_api

if __name__=="__main__":
    # Specify the API endpoint, headers, and directory to save the file
    params = "/leagues?id=2"  # Change as needed
    # params = "/leagues"
    call_api(params)
