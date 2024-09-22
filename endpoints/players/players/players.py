from connection import call_api

if __name__=="__main__":
    # Specify the API endpoint, headers, and directory to save the file
    params = '/players?team=66&season=2023'  # Change as needed

    # params = "/leagues"
    call_api(params)
