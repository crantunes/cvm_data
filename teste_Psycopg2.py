try:
    import psycopg2
    print("✅ Psycopg2 encontrado e carregado!")
except ImportError:
    print("❌ Ainda não encontrado. Tente: pip install psycopg2-binary")