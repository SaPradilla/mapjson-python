import ijson
import pandas as pd
import os
from tqdm import tqdm

# Create the 'conversations' folder if it does not exist
output_directory = 'conversations'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
# Count the total number of messages in the JSON file
message_count = 0
with open('data.json', 'r', encoding='utf-8') as file:
    parser = ijson.parse(file)
    for prefix, event, value in parser:
        if prefix.endswith('.MessageList.item') and event == 'end_map':
            message_count += 1

# Open json file
with open('data.json', 'r', encoding='utf-8') as file:
    parser = ijson.parse(file)
    
    # temp vars
    message_data = {}
    current_field = None
    messages = []
    # Number of messages per block
    batch_size = 10000
    batch_number = 0
    total_messages = 0

    print("Starting JSON file processing ^_^")

    # Initial bar progress
    with tqdm(total=message_count, desc="Processing messages") as pbar:
        for prefix, event, value in parser:
            if prefix.endswith('.MessageList.item') and event == 'start_map':
                message_data = {}

            if prefix.endswith('.MessageList.item') and event == 'end_map':
                messages.append(message_data)
                total_messages += 1
                 
                # Updates the progress bar
                pbar.update(1) 

                
                # If we reach the block size, we process and save

                if len(messages) == batch_size:
                    df = pd.DataFrame(messages)
                    try:
                        output_path = os.path.join(output_directory, f'conversations_{batch_number}.xlsx')
                        df.to_excel(output_path, index=False)
                    except Exception as e:
                        print(f"Error saving the file: {e}")
                    batch_number += 1
                    messages = []

            if event == 'map_key':
                current_field = value

            elif current_field == 'displayName' and event == 'string':
                message_data['name'] = value
            elif current_field == 'messagetype' and event == 'string':
                message_data['messagetype'] = value
            elif current_field == 'content' and event == 'string':
                message_data['message'] = value
            elif current_field == 'from' and event == 'string':
                message_data['from'] = value
            elif current_field == 'originalarrivaltime' and event == 'string':
                message_data['date'] = pd.to_datetime(value).tz_localize(None)

        # Process any remaining messages
        if messages:
            df = pd.DataFrame(messages)
            try:
                output_path = os.path.join(output_directory, f'conversations_{batch_number}.xlsx')
                df.to_excel(output_path, index=False)
            except Exception as e:
                print(f"Error saving the file: {e}")

print("Processing completed :-D")