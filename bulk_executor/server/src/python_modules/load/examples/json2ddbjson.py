import json
import boto3
from boto3.dynamodb.types import TypeSerializer

def convert_to_dynamodb_json(plain_json, number_attributes=None):
    """
    Convert a plain JSON object to DynamoDB JSON format with specific type handling.

    Args:
        plain_json (dict): A plain JSON object
        number_attributes (list): List of attribute names to be converted to numbers

    Returns:
        dict: The JSON object in DynamoDB format
    """
    # Make sure we're working with a dictionary
    if not isinstance(plain_json, dict):
        raise TypeError("Input must be a dictionary, not a list or other type")

    # Default to empty list if None
    number_attributes = number_attributes or []

    # Convert specified string attributes to numbers
    for attr in number_attributes:
        if attr in plain_json and isinstance(plain_json[attr], str):
            try:
                # Convert string to int or float as appropriate
                if plain_json[attr].isdigit():
                    plain_json[attr] = int(plain_json[attr])
                else:
                    plain_json[attr] = float(plain_json[attr])
            except ValueError:
                print(f"Warning: Could not convert '{attr}' to number, keeping as string")

    # Use TypeSerializer to convert to DynamoDB format
    serializer = TypeSerializer()
    dynamodb_json = {k: serializer.serialize(v) for k, v in plain_json.items()}
    return dynamodb_json

def main():
    # Example usage
    try:
        with open('persons.json', 'r') as file:
            data = json.load(file)

        # Check if data is a list instead of a dictionary
        if isinstance(data, list):
            # Handle list case - process each item individually
            results = []
            for item in data:
                results.append(convert_to_dynamodb_json(item, ["age"]))

            # Save the list of converted items to a file
            with open('persons.ddb.json', 'w') as file:
                json.dump(results, file, indent=2)
                print("Converted JSON list saved to persons.ddb.json")

            return results
        else:
            # Process single dictionary
            result = convert_to_dynamodb_json(data, ["age"])

            # Save the converted item to a file
            with open('persons.ddb.json', 'w') as file:
                json.dump(result, file, indent=2)
                print("Converted JSON saved to persons.ddb.json")

            return result

    except FileNotFoundError:
        print("Error: input.json file not found")
    except json.JSONDecodeError:
        print("Error: Invalid JSON in input file")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
