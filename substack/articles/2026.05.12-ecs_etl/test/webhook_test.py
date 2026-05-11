from helpers.common import send_slack_notification

def main():
    send_slack_notification("info", "This is a test notification from the webhook test script.")    

if __name__ == "__main__":
    main()