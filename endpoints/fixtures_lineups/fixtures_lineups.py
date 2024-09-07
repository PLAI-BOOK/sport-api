from connection import call_api

if __name__=="__main__":
    # Specify the API endpoint, headers, and directory to save the file
    params = "/fixtures/lineups?fixture=1216981"  # Change as needed
    call_api(params)