from connection import call_api

if __name__=="__main__":
    # Specify the API endpoint, headers, and directory to save the file
    params = "/teams?id=50"  # Change as needed
    call_api(params)

