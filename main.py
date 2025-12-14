import os
import json
import io
import re
import datetime
import requests
import unicodedata
from google.cloud import pubsub_v1

# ===== Novo: suporte opcional a Redis =====
try:
    import redis
except Exception:
    redis = None

def _init_redis():
    """Inicializa cliente Redis opcional a partir de REDIS_URL (pode ser redis://...)."""
    url = os.environ.get("REDIS_URL")
    if not url or redis is None:
        if not url:
            print("⚠ REDIS_URL não configurado, cache desativado")
        else:
            print("⚠ Biblioteca 'redis' não disponível, cache desativado")
        return None
    try:
        client = redis.Redis.from_url(url, decode_responses=True)
        # testar ligação simples
        client.ping()
        print("✓ Redis ligado:", url)
        return client
    except Exception as e:
        print(f"✗ Falha ao ligar ao Redis ({e}), cache desativado")
        return None

redis_client = _init_redis()

def _remove_accents(text):
    """Remove acentos de uma string"""
    if not text:
        return text
    # Normaliza para NFD (separa caracteres base dos diacríticos)
    nfd = unicodedata.normalize('NFD', text)
    # Remove os diacríticos (categoria Mn = Nonspacing Mark)
    return ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')

def send_text_file_to_discord(webhook_url, text, filename="client.txt"):
    """Envia JSON como mensagem para Discord via webhook (sem ficheiro)."""
    try:
        # Envia o JSON no corpo como content (sem anexos)
        resp = requests.post(webhook_url, json={"content": text}, timeout=15)
        resp.raise_for_status()
        print(f"✓ Mensagem JSON enviada para Discord ({resp.status_code})")
        return True
    except Exception as e:
        print(f"✗ Erro ao enviar mensagem para Discord: {e}")
        return False

def _safe_filename(name):
    """Gera um nome de ficheiro seguro com timestamp"""
    name = (name or "cliente").strip()
    # Remove acentos antes de criar o nome do ficheiro
    name = _remove_accents(name)
    name = re.sub(r"[^A-Za-z0-9\-_\s]", "_", name)
    name = name.replace(" ", "_")[:50]  # limitar tamanho
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"cliente_{name}_{ts}.txt"

def _format_client_text(client):
    """Formata os dados do cliente em JSON"""
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Cria uma cópia do cliente com acentos removidos
    client_normalized = {}
    for key, value in client.items():
        if isinstance(value, str):
            client_normalized[key] = _remove_accents(value)
        else:
            client_normalized[key] = value
    
    client_json = {
        "name": client_normalized.get("name"),
        "email": client_normalized.get("email"),
        "phone": client_normalized.get("phone"),
        "company": client_normalized.get("company"),
        "address": client_normalized.get("address"),
        "preferred_contact": client_normalized.get("preferred_contact"),
        "notes": client_normalized.get("notes"),
        "created_at": now
    }
    
    return json.dumps(client_json, indent=2, ensure_ascii=False)

def _client_cache_key(client):
    """Gera chave de cache baseada em email/phone/nome (normalizado)."""
    identifier = client.get("email") or client.get("phone") or client.get("name") or "unknown"
    identifier = _remove_accents(str(identifier)).lower()
    identifier = re.sub(r"[^a-z0-9_]", "_", identifier)[:64]
    return f"client_cache:{identifier}"

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
        
        # enviar para Discord (com cache Redis opcional)
        webhook_url = os.environ.get("DISCORD_URL")
        if not webhook_url:
            print("⚠ DISCORD_URL não configurado, a ignorar envio")
        else:
            filename = _safe_filename(client_data.get("name"))
            text = _format_client_text(client_data)

            # Verificar cache
            if redis_client:
                try:
                    key = _client_cache_key(client_data)
                    if redis_client.get(key):
                        print(f"✓ Cliente já em cache ({key}), a ignorar envio para Discord")
                    else:
                        if send_text_file_to_discord(webhook_url, text, filename=filename):
                            # TTL configurável via env REDIS_TTL (segundos), padrão 3600s
                            ttl = int(os.environ.get("REDIS_TTL", "3600"))
                            try:
                                redis_client.set(key, "1", ex=ttl)
                                print(f"✓ Cache gravado: {key} (TTL={ttl}s)")
                            except Exception as e:
                                print(f"⚠ Não foi possível gravar cache: {e}")
                        else:
                            print(f"✗ Falha ao processar cliente: {client_data.get('name')}")
                except Exception as e:
                    print(f"⚠ Erro durante operações de cache: {e}")
                    # fallback: tentar enviar sem cache
                    if send_text_file_to_discord(webhook_url, text, filename=filename):
                        print(f"✓ Cliente processado (sem cache): {client_data.get('name')}")
            else:
                # Sem Redis -> comportamento original
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