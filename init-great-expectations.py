#!/usr/bin/env python3
"""
Script de inicialização do Great Expectations
Executa automaticamente no container para configurar o ambiente
"""

import os
import sys
import subprocess

def install_great_expectations():
    """Instala Great Expectations se não estiver disponível"""
    try:
        import great_expectations as gx
        print(f"✅ Great Expectations já instalado: {gx.__version__}")
        return True
    except ImportError:
        print("📦 Instalando Great Expectations...")
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "great-expectations==0.18.8", 
                "sqlalchemy==1.4.46"
            ])
            print("✅ Great Expectations instalado com sucesso!")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Erro na instalação: {e}")
            return False

def initialize_data_context():
    """Inicializa o Data Context do Great Expectations"""
    try:
        import great_expectations as gx
        
        # Diretório para o Great Expectations
        ge_dir = "/home/tavares/work/great_expectations"
        
        if not os.path.exists(ge_dir):
            print("🔧 Inicializando Data Context...")
            context = gx.get_context(project_root_dir="/home/tavares/work")
            print("✅ Data Context inicializado!")
        else:
            print("✅ Data Context já existe!")
            
        return True
    except Exception as e:
        print(f"❌ Erro ao inicializar Data Context: {e}")
        return False

def main():
    """Função principal"""
    print("🚀 Configurando Great Expectations...")
    
    if install_great_expectations():
        initialize_data_context()
        print("✅ Great Expectations configurado com sucesso!")
    else:
        print("❌ Falha na configuração do Great Expectations")
        sys.exit(1)

if __name__ == "__main__":
    main()