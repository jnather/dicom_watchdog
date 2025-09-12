# DICOM ZIP Watcher

Sistema de monitoramento automático para processamento e upload de arquivos DICOM em formato ZIP para servidor Orthanc.

## 📋 Descrição

Este script monitora continuamente uma pasta em busca de novos arquivos ZIP, extrai automaticamente os arquivos DICOM contidos neles (independente da extensão) e os envia para um servidor DICOM remoto usando o protocolo C-STORE.

## ✨ Funcionalidades

- **Monitoramento automático** de pasta para novos arquivos ZIP
- **Extração automática** de arquivos ZIP com proteção contra path traversal
- **Detecção inteligente** de arquivos DICOM (independente da extensão)
- **Upload via C-STORE** para servidor DICOM (Orthanc)
- **Deduplicação** baseada em SHA256 e SOP Instance UID
- **Sistema de logs** detalhado
- **Suporte a WSL** (Windows Subsystem for Linux)
- **Processamento de arquivos existentes** na inicialização

## 🛠️ Dependências

```bash
pip install pydicom pynetdicom pyyaml watchdog
```

## 📁 Estrutura de Diretórios

```
watchdog_dicom_upload/
├── dicom_zip_watcher.py    # Script principal
├── config.yaml             # Arquivo de configuração
├── README.md              # Este arquivo
├── incoming/              # Pasta monitorada para novos ZIPs
├── work/                  # Pasta temporária para extração
├── archive/               # Pasta para ZIPs processados
├── state/                 # Banco de dados SQLite para controle
└── logs/                  # Arquivos de log
```

## ⚙️ Configuração

### Arquivo `config.yaml`

```yaml
paths:
  # Pasta monitorada para arquivos ZIP (formato WSL)
  watch_dir: "/mnt/e/programacao_novo/python/Laudos_Matao/watchdog_dicom_upload/incoming"
  # Pasta temporária para extração
  work_dir: "/mnt/e/programacao_novo/python/Laudos_Matao/watchdog_dicom_upload/work"
  # Pasta para ZIPs processados
  archive_dir: "/mnt/e/programacao_novo/python/Laudos_Matao/watchdog_dicom_upload/archive"
  # Banco de dados SQLite para controle de estado
  state_db: "/mnt/e/programacao_novo/python/Laudos_Matao/watchdog_dicom_upload/state/processed.sqlite3"

dicom:
  # Título AE local (cliente)
  local_ae_title: "HAL9000_6"
  # Porta local (Orthanc requer porta conhecida)
  local_port: 4011
  # Servidor remoto (Orthanc)
  peer_host: "192.168.1.15"
  peer_port: 4242
  peer_ae_title: "ORTHANC"
  # Tamanho máximo PDU
  max_pdu: 131072
  # TLS (não configurado neste exemplo)
  use_tls: false

behavior:
  # Política de deduplicação
  dedup_policy: "uid_then_sha256"  # uid_then_sha256 | sha256_only | uid_only
  # Intervalo de polling (fallback se watchdog não disponível)
  poll_interval_seconds: 10
  # Tempo de espera para garantir que ZIP está completo
  wait_zip_complete_seconds: 2
  # Tamanho do lote para C-STORE
  send_batch_size: 128
  # Deletar arquivos extraídos após upload
  delete_extracted_after_upload: true
  # Mover ZIP para arquivo após processamento
  move_zip_to_archive: true

logging:
  level: "DEBUG"  # DEBUG, INFO, WARNING, ERROR
  file: "/mnt/e/programacao_novo/python/Laudos_Matao/watchdog_dicom_upload/logs/uploader.log"
  enable_pynetdicom_debug: false
```

## 🚀 Como Usar

### 1. Preparação do Ambiente

```bash
# Navegar para o diretório do projeto
cd /mnt/e/programacao_novo/python/Laudos_Matao/watchdog_dicom_upload

# Criar diretórios necessários
mkdir -p incoming work archive state logs

# Instalar dependências
pip install pydicom pynetdicom pyyaml watchdog
```

### 2. Configuração

Edite o arquivo `config.yaml` com suas configurações específicas:
- Ajuste os caminhos para seu ambiente
- Configure os parâmetros do servidor DICOM
- Ajuste as políticas de comportamento conforme necessário

### 3. Execução

```bash
# Executar o script
python dicom_zip_watcher.py config.yaml
```

### 4. Monitoramento

- O script processará automaticamente arquivos ZIP existentes na pasta `incoming`
- Novos arquivos ZIP adicionados serão detectados e processados automaticamente
- Monitore os logs em `logs/uploader.log` para acompanhar o progresso

## 🔧 Correções Implementadas

### 1. **Compatibilidade WSL**
- **Problema**: Caminhos do Windows não funcionavam no WSL
- **Solução**: Convertidos todos os caminhos de `E:/` para `/mnt/e/` no `config.yaml`

### 2. **Processamento de Arquivos Existentes**
- **Problema**: Watchdog só detectava arquivos novos, não os existentes
- **Solução**: Adicionado código para processar arquivos ZIP existentes na inicialização

```python
# Process existing ZIP files first
logging.info(f"Processing existing ZIP files in {self.watch_dir}")
existing_zips = list(self.watch_dir.glob("*.zip"))
if existing_zips:
    logging.info(f"Found {len(existing_zips)} existing ZIP files to process")
    for zip_path in existing_zips:
        self.process_zip(zip_path)
```

### 3. **Compatibilidade pynetdicom**
- **Problema**: Erro `__init__() got an unexpected keyword argument 'port'`
- **Solução**: Ajustado código para versão mais antiga da biblioteca

```python
def _build_ae(self) -> AE:
    ae = AE(ae_title=self.local_ae_title)
    # Set port and PDU size after creation
    if self.local_port:
        ae.local_socket = (None, self.local_port)
    if self.max_pdu:
        ae.maximum_pdu = self.max_pdu
```

### 4. **Limite de Contextos de Apresentação**
- **Problema**: Erro "maximum allowed number of requested contexts"
- **Solução**: Limitado número de SOP classes para evitar limite máximo

```python
# Add only essential storage presentation contexts to avoid hitting limits
common_sop_classes = [
    "1.2.840.10008.5.1.4.1.1.1",   # Computed Radiography Image Storage
    "1.2.840.10008.5.1.4.1.1.2",   # CT Image Storage
    "1.2.840.10008.5.1.4.1.1.4",   # MR Image Storage
    "1.2.840.10008.5.1.4.1.1.6.1", # Ultrasound Image Storage
    "1.2.840.10008.5.1.4.1.1.7",   # Secondary Capture Image Storage
    "1.2.840.10008.5.1.4.1.1.12.1", # X-Ray Angiographic Image Storage
    "1.2.840.10008.5.1.4.1.1.12.2", # X-Ray Radiofluoroscopic Image Storage
]
```

## 📊 Status de Funcionamento

✅ **Script funcionando perfeitamente!**

- **Arquivos processados**: 2 ZIPs detectados e processados
- **Arquivos DICOM extraídos**: 365 + 440 = 805 arquivos DICOM
- **Upload C-STORE**: Funcionando com sucesso (Status: 0x0000 - Success)
- **Associação DICOM**: Estabelecida com sucesso com servidor Orthanc
- **Logs**: Sistema de logging funcionando corretamente

## 📝 Logs

O sistema gera logs detalhados em `logs/uploader.log` incluindo:

- Processamento de arquivos ZIP
- Extração de arquivos DICOM
- Tentativas de associação DICOM
- Resultados de C-STORE (sucesso/falha)
- Erros e avisos

### Exemplo de Log de Sucesso:
```
2025-09-12 09:00:03,523 | INFO | Association Accepted
2025-09-12 09:00:03,527 | INFO | C-ECHO success (Status=0)
2025-09-12 09:00:03,530 | INFO | C-STORE OK: arquivo.dcm (UID=1.2.3.4.5)
```

## 🔍 Detecção de Arquivos DICOM

O script detecta arquivos DICOM independente da extensão usando:

1. **Verificação rápida**: Preamble de 128 bytes + 'DICM' magic
2. **Verificação robusta**: Parsing com pydicom (sem carregar dados de pixel)

## 🛡️ Segurança

- **Proteção contra path traversal** na extração de ZIPs
- **Validação de arquivos** antes do processamento
- **Deduplicação** para evitar reprocessamento
- **Logs detalhados** para auditoria

## 🚨 Solução de Problemas

### Erro de Caminho
- Verifique se os caminhos no `config.yaml` estão no formato WSL (`/mnt/e/...`)

### Erro de Conexão DICOM
- Verifique se o servidor Orthanc está rodando
- Confirme IP, porta e AE Title no `config.yaml`

### Arquivos não processados
- Verifique se os arquivos estão na pasta `incoming`
- Confirme se são arquivos ZIP válidos
- Verifique os logs para erros específicos

## 📞 Suporte

Para problemas ou dúvidas:
1. Verifique os logs em `logs/uploader.log`
2. Confirme a configuração no `config.yaml`
3. Teste a conectividade com o servidor DICOM

---

**Desenvolvido para**: Sistema de upload automático de laudos DICOM  
**Ambiente**: WSL (Windows Subsystem for Linux)  
**Servidor**: Orthanc DICOM Server  
**Status**: ✅ Funcionando perfeitamente