from connection import call_api

if __name__=="__main__":
    # Specify the API endpoint, headers, and directory to save the file
    # params = "/fixtures/statistics?fixture=1216981"  # Change as needed
    params ='/fixtures/statistics?fixture=1027909'
    call_api(params)
