import ijson
import pandas as pd

with open('archivo.json', 'r', encoding='utf-8') as file:
    parser = ijson.parse(file)
    
    # Variables para almacenar datos temporales
    message_data = {}
    current_field = None
    messages = []
     # Número de mensajes por bloque
    batch_size = 10000 
    batch_number = 0
    total_messages = 0

    print("Iniciando el procesamiento del archivo JSON...")

    # Recorre el parser
    for prefix, event, value in parser:
        if prefix.endswith('.MessageList.item') and event == 'start_map':
            message_data = {}

        if prefix.endswith('.MessageList.item') and event == 'end_map':
            messages.append(message_data)
            total_messages += 1

            # Si alcanzamos el tamaño del bloque, procesamos y guardamos
            if len(messages) == batch_size:
                print(f"Procesando bloque {batch_number + 1}, mensajes totales procesados: {total_messages}")
                df = pd.DataFrame(messages)
                df.to_excel(f'conversaciones_{batch_number}.xlsx', index=False)
                print(f"Bloque {batch_number + 1} guardado como 'conversaciones_{batch_number}.xlsx'")
                batch_number += 1
                messages = []

        if event == 'map_key':
            current_field = value
        elif current_field == 'id' and event == 'number':
            message_data['id'] = value
        elif current_field == 'displayName' and event == 'string':
            message_data['displayName'] = value
        elif current_field == 'originalarrivaltim' and event == 'string':
            message_data['originalarrivaltim'] = value
        elif current_field == 'messagetype' and event == 'string':
            message_data['messagetype'] = value
        elif current_field == 'version' and event == 'number':
            message_data['version'] = value
        elif current_field == 'content' and event == 'string':
            message_data['content'] = value
        elif current_field == 'conversationid' and event == 'string':
            message_data['conversationid'] = value
        elif current_field == 'from' and event == 'string':
            message_data['from'] = value

    # Procesa cualquier mensaje restante
    if messages:
        print(f"Procesando último bloque, mensajes totales procesados: {total_messages}")
        df = pd.DataFrame(messages)
        df.to_excel(f'conversaciones_{batch_number}.xlsx', index=False)
        print(f"Último bloque guardado como 'conversaciones_{batch_number}.xlsx'")

print("Procesamiento completado.")