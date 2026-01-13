import json
import pandas as pd
from datetime import datetime, timedelta
import csv
import os

def convert_to_brasilian_time(utc_time_str):
    '''
    Converts a UTC time string to Brasilian timezone (UTC-3) 
    and returns it in DD/MM/YYYY format.
    '''
    # Format in JSON: "2021-09-05 08:04:08 UTC"
    utc_time_str = utc_time_str.replace(" UTC", "")
    dt_utc = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
    dt_sp = dt_utc - timedelta(hours=3)
    return dt_sp.strftime("%d/%m/%Y")

def process_case_data(input_file, output_dir):
    with open(input_file, 'r') as f:
        data = json.load(f)

    curated_offer_options = []
    dynamic_price_option = []
    dynamic_price_range = []

    for record in data:
        enqueued_time_utc = record.get('EnqueuedTimeUtc')
        enqueued_time_sp = convert_to_brasilian_time(enqueued_time_utc)
        event_name = record.get('EventName')
        payload_str = record.get('Payload')
        
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            print(f"Failed to decode payload for record: {record}")
            continue

        if event_name == "DynamicPrice_Result":
            provider = payload.get('provider')
            offer_id = payload.get('offerId')
            algorithm_output = payload.get('algorithmOutput')

            if provider == "ApplyDynamicPriceRange":
                dynamic_price_range.append({
                    "Provider": provider,
                    "OfferId": offer_id,
                    "MinGlobal": algorithm_output.get('min_global'),
                    "MinRecommended": algorithm_output.get('min_recommended'),
                    "MaxRecommended": algorithm_output.get('max_recommended'),
                    "DifferenceMinRecommendMinTheory": algorithm_output.get('differenceMinRecommendMinTheory'),
                    "EnqueuedTimeSP": enqueued_time_sp
                })
            
            elif provider == "ApplyDynamicPricePerOption":
                # algorithm_output is a list
                for opt in algorithm_output:
                    dynamic_price_option.append({
                        "Provider": provider,
                        "OfferId": offer_id,
                        "UniqueOptionId": opt.get('uniqueOptionId'),
                        "BestPrice": opt.get('bestPrice'),
                        "EnqueuedTimeSP": enqueued_time_sp
                    })

        elif event_name == "CurateOffer_Result":
            # payload is a list of curated results
            for curate_item in payload:
                curation_provider = curate_item.get('curationProvider')
                offer_id = curate_item.get('offerId')
                dealer_id = curate_item.get('dealerId')
                options = curate_item.get('options', [])
                
                for opt in options:
                    # Handle DefeatReasons as a string representation or join them
                    defeat_reasons = opt.get('defeatReasons')
                    if isinstance(defeat_reasons, list):
                        defeat_reasons = ",".join(defeat_reasons)
                    
                    curated_offer_options.append({
                        "CurationProvider": curation_provider,
                        "OfferId": offer_id,
                        "DealerId": dealer_id,
                        "UniqueOptionId": opt.get('uniqueOptionId'),
                        "OptionId": opt.get('optionId'),
                        "IsMobileDealer": opt.get('isMobileDealer'),
                        "IsOpen": opt.get('isOpen'),
                        "Eta": opt.get('eta'),
                        "ChamaScore": opt.get('chamaScore'),
                        "ProductBrand": opt.get('productBrand'),
                        "IsWinner": opt.get('isWinner'),
                        "MinimumPrice": opt.get('minimumPrice'),
                        "MaximumPrice": opt.get('maximumPrice'),
                        "DynamicPrice": opt.get('dynamicPrice'),
                        "FinalPrice": opt.get('finalPrice'),
                        "DefeatPrimaryReason": opt.get('defeatPrimaryReason', ""),
                        "DefeatReasons": defeat_reasons if defeat_reasons else "",
                        "EnqueuedTimeSP": enqueued_time_sp
                    })

    # Save to CSVs
    os.makedirs(output_dir, exist_ok=True)
    
    # We use csv module to ensure specific quoting if needed, 
    # but Pandas with quoting=csv.QUOTE_NONNUMERIC handles "strings in quotes, others not" well.
    # Note: Boolean values in Pandas might be converted to strings unless handled.
    
    df1 = pd.DataFrame(curated_offer_options)
    df2 = pd.DataFrame(dynamic_price_option)
    df3 = pd.DataFrame(dynamic_price_range)

    # To strictly follow "without quotes" for Booleans, we should ensure they are not strings.
    # QUOTE_NONNUMERIC will quote anything that is not a float or int. 
    # Booleans are often treated as integers (1/0) or strings in CSV if not careful.
    
    def save_with_custom_quoting(df, filename):
        if df.empty:
            return
        path = os.path.join(output_dir, filename)
        # QUOTE_NONNUMERIC: Quotes all non-numeric fields.
        # This usually includes strings and dates, but we need to see how it handles Booleans.
        df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
        print(f"Saved {filename}")

    save_with_custom_quoting(df1, "CuratedOfferOptions.csv")
    save_with_custom_quoting(df2, "DynamicPriceOption.csv")
    save_with_custom_quoting(df3, "DynamicPriceRange.csv")

if __name__ == "__main__":
    # Get the directory where the script is located
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Use relative paths from the script directory
    input_json = os.path.join(base_dir, "datasets", "case.json")
    output_directory = os.path.join(base_dir, "output")
    
    process_case_data(input_json, output_directory)
