import json
import sys
import pandas as pd
import os

file_path = 'C:/Users/chris/Documents/tiaa/gtm-data-dictionary/gtm-data-dictionary/json/GTM-NP77X9R_workspace296.json'

def parse_gtm_json(file_path):
    with open(file_path, encoding='utf-8') as file:
        data = json.load(file)

    tags = data.get("containerVersion", {}).get("tag", [])
    triggers = data.get("containerVersion", {}).get("trigger", [])

    matching_tags = []
    matching_triggers = []
    trigger_ids = []
    for tag in tags:
        tag_details = {
            "tag_name": tag.get("name"),
            "event_name": "",
            "marketing_category": "",
            "marketing_action": "",
            "marketing_label": "",
            "triggers": tag.get("firingTriggerId"),
        }

        for param in tag.get("parameter", []):
            key = param.get("key")

            if key == "eventName":
                tag_details["event_name"] = param.get("value", "")
            # Check if current parameter is 'eventSettingsTable' to find marketing info
            elif key == "eventSettingsTable":
                for setting in param.get("list", []):
                    for item in setting.get("map", []):
                        if (
                            item.get("key") == "parameter"
                            and item.get("value") == "marketing_category"
                        ):
                            # Extract next item as it contains the value for marketing_category
                            tag_details["marketing_category"] = next(
                                (
                                    i["value"]
                                    for i in setting["map"]
                                    if i.get("key") == "parameterValue"
                                ),
                                "",
                            )
                        elif (
                            item.get("key") == "parameter"
                            and item.get("value") == "marketing_action"
                        ):
                            tag_details["marketing_action"] = next(
                                (
                                    i["value"]
                                    for i in setting["map"]
                                    if i.get("key") == "parameterValue"
                                ),
                                "",
                            )
                        elif (
                            item.get("key") == "parameter"
                            and item.get("value") == "marketing_label"
                        ):
                            tag_details["marketing_label"] = next(
                                (
                                    i["value"]
                                    for i in setting["map"]
                                    if i.get("key") == "parameterValue"
                                ),
                                "",
                            )

        # Check if all required information is available for the tag
        if all(tag_details.values()):
            matching_tags.append(tag_details)
            trigger_ids += tag.get("firingTriggerId")

    for trigger in triggers:
        if trigger.get("triggerId") in trigger_ids:
            trigger_details = {
                "triggerId": trigger.get("triggerId"),
                "name": trigger.get("name"),
                "type": trigger.get("type"),
                "filter": trigger.get("filter"),
            }
            matching_triggers.append(trigger_details)

    return [matching_tags, matching_triggers]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python gtm_ga4_summary.py <path_to_json_file>")
        sys.exit(1)

    file_path = sys.argv[1]
    [tags, triggers] = parse_gtm_json(file_path)
    print(json.dumps(tags, indent=4))

    tags_df = pd.DataFrame(tags)
    triggers_df = pd.DataFrame(triggers)

    # Specify the 'output' directory at the same level as the script's directory
    output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'output')

    # Create the 'output' directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Construct the file paths within the 'output' directory
    tags_csv_file_path = os.path.join(output_directory, f"{os.path.basename(file_path).replace('.json','_tags.csv')}")
    triggers_csv_file_path = os.path.join(output_directory, f"{os.path.basename(file_path).replace('.json','_triggers.csv')}")

    tags_df.to_csv(tags_csv_file_path, index=False)
    triggers_df.to_csv(triggers_csv_file_path, index=False)

    print(f"Exported {len(tags_df)} tags to '{tags_csv_file_path}'")
    print(f"Exported {len(triggers_df)} triggers to '{triggers_csv_file_path}'")
