import sys
import csv
import json
from io import StringIO

def csv_to_json(data_input):
    csv_stream = StringIO(data_input)
    reader = csv.DictReader(csv_stream, delimiter=',')
    data = []
    
    for row in reader:
        data.append(row)
        
    return json.dumps(data, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    try:
        input_data = sys.stdin.read()
        
        if not input_data:
            print("Erro: Nenhuma entrada de dados fornecida. O script espera dados via pipe (e.g., cat file.csv | python3 script.py).", file=sys.stderr)
            sys.exit(1)
        json_output = csv_to_json(input_data)
        print(json_output)
        
    except Exception as e:
        print(f"Ocorreu um erro ao processar os dados: {e}", file=sys.stderr)
        sys.exit(1)