# Subir nós locais para desenvolvimento rápido.
param(
  [int]$Nodes = 3,
  [int]$BasePort = 8000
)

Write-Host "Subindo $Nodes nós, base port $BasePort"

$procs = @()
for ($i = 1; $i -le $Nodes; $i++) {
  $port = $BasePort + $i - 1
  $peerList = @()
  for ($j = 1; $j -le $Nodes; $j++) {
    if ($j -eq $i) { continue }
    $peerPort = $BasePort + $j - 1
    $peerList += "127.0.0.1:$peerPort:$j"
  }
  $peers = ($peerList -join ",")
  $cmd = "python -m src.node.main --id $i --port $port --peers $peers"
  Write-Host "Start: $cmd"
  $procs += Start-Process -FilePath "python" -ArgumentList "-m src.node.main --id $i --port $port --peers $peers" -PassThru
}

Write-Host "Pressione ENTER para encerrar todos os nós..."
[void][System.Console]::ReadLine()

foreach ($p in $procs) {
  Stop-Process -Id $p.Id -Force
}
