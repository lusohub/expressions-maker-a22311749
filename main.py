import os
import json
import io
import re
import datetime
import requests
from google.cloud import pubsub_v1

def send_text_file_to_discord(webhook_url, text, filename="client.txt"):
    """Envia ficheiro de texto para Discord via webhook"""
    try:
        file_bytes = io.BytesIO(text.encode("utf-8"))
        file_bytes.seek(0)
        files = {"file": (filename, file_bytes, "text/plain")}
        resp = requests.post(webhook_url, files=files, timeout=15)
        resp.raise_for_status()
        print(f"✓ Ficheiro enviado para Discord: {filename} ({resp.status_code})")
        return True
    except Exception as e:
        print(f"✗ Erro ao enviar ficheiro para Discord: {e}")
        return False

def _safe_filename(name):
    """Gera um nome de ficheiro seguro com timestamp"""
    name = (name or "cliente").strip()
    name = re.sub(r"[^A-Za-z0-9\-_\s]", "_", name)
    name = name.replace(" ", "_")[:50]  # limitar tamanho
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"cliente_{name}_{ts}.txt"

def _format_client_text(client):
    """Formata os dados do cliente em texto legível"""
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    lines = [
        "=" * 50,
        "REGISTO DE CLIENTE",
        "=" * 50,
        f"Data de criação: {now}",
        "",
        "INFORMAÇÃO DO CLIENTE",
        "-" * 50,
    ]
    
    fields = [
        ("Nome", client.get("name")),
        ("Email", client.get("email")),
        ("Telefone", client.get("phone")),
        ("Empresa", client.get("company")),
        ("Morada", client.get("address")),
        ("Contacto Preferido", client.get("preferred_contact")),
        ("Notas", client.get("notes")),
    ]
    
    for label, value in fields:
        if value:
            lines.append(f"{label}: {value}")
        else:
            lines.append(f"{label}: (não fornecido)")
    
    lines.extend([
        "",
        "=" * 50,
        "Fim do registo",
        "=" * 50
    ])
    
    return "\n".join(lines)

def callback(message):
    """Processa mensagens do Pub/Sub"""
    print(f"\n{'='*60}")
    print(f"Nova mensagem recebida: {message.message_id}")
    print(f"{'='*60}")
    
    try:
        # decodificar dados
        raw = message.data
        client_data = {}
        
        try:
            decoded = json.loads(raw.decode("utf-8"))
            print(f"Payload JSON: {json.dumps(decoded, indent=2, ensure_ascii=False)}")
            
            # extrair dados do cliente
            if isinstance(decoded, dict):
                if "client" in decoded:
                    client_data = decoded["client"]
                else:
                    client_data = decoded
            else:
                print("⚠ Formato inesperado, a usar como nota")
                client_data = {"notes": str(decoded)}
                
        except json.JSONDecodeError as e:
            print(f"⚠ Erro ao decodificar JSON: {e}")
            text = raw.decode("utf-8", errors="replace")
            client_data = {"notes": text}
        
        # validar dados mínimos
        if not client_data.get("name"):
            print("⚠ Nome do cliente não encontrado, a usar mensagem como nota")
            if not client_data.get("notes"):
                client_data["notes"] = str(client_data)
            client_data["name"] = "Cliente_Sem_Nome"
        
        print(f"\nCliente: {client_data.get('name')}")
        print(f"Email: {client_data.get('email') or '(não fornecido)'}")
        print(f"Telefone: {client_data.get('phone') or '(não fornecido)'}")
        
        # enviar para Discord
        webhook_url = os.environ.get("DISCORD_URL")
        if not webhook_url:
            print("⚠ DISCORD_URL não configurado, a ignorar envio")
        else:
            filename = _safe_filename(client_data.get("name"))
            text = _format_client_text(client_data)
            
            if send_text_file_to_discord(webhook_url, text, filename=filename):
                print(f"✓ Cliente processado com sucesso: {client_data.get('name')}")
            else:
                print(f"✗ Falha ao processar cliente: {client_data.get('name')}")
        
        message.ack()
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"✗ ERRO ao processar mensagem: {e}")
        import traceback
        traceback.print_exc()
        message.nack()

def main():
    """Inicia o consumer Pub/Sub"""
    project_id = os.environ.get("GCP_PROJECT_ID")
    subscription_id = os.environ.get("PUBSUB_SUBSCRIPTION_ID")

    if not project_id or not subscription_id:
        print("✗ Erro: GCP_PROJECT_ID e PUBSUB_SUBSCRIPTION_ID devem estar configurados")
        return

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    print("=" * 60)
    print("CONSUMER DE CLIENTES - INICIADO")
    print("=" * 60)
    print(f"Projeto: {project_id}")
    print(f"Subscrição: {subscription_id}")
    print(f"Path: {subscription_path}")
    print(f"Discord: {'Configurado' if os.environ.get('DISCORD_URL') else 'NÃO configurado'}")
    print("=" * 60)
    print("\nA aguardar mensagens...\n")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\n\n✓ Consumer parado pelo utilizador")

if __name__ == "__main__":
    main()