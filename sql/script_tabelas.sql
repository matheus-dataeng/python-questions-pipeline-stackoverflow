CREATE TABLE IF NOT EXISTS dim_usuario (
    id_usuario INT PRIMARY KEY,
    id_conta_usuario INT,
    nome_usuario TEXT,
    tipo_usuario TEXT,
    reputacao_usuario INT,
    taxa_aceitacao_usuario FLOAT
);

CREATE TABLE IF NOT EXISTS dim_tempo (
    id_tempo INT PRIMARY KEY,
    data_criacao TIMESTAMP,
    ano INT,
    mes INT,
    dia INT,
    hora INT,
    dia_semana TEXT
);

CREATE TABLE IF NOT EXISTS dim_tags (
    id_tags BIGINT PRIMARY KEY,
    tags    TEXT
);

CREATE TABLE IF NOT EXISTS dim_perguntas (
    id_pergunta INT PRIMARY KEY,
    titulo TEXT,
    licenca_conteudo TEXT
);

CREATE TABLE IF NOT EXISTS fato_perguntas (
    id_pergunta INT,
    id_usuario INT,
    id_tempo INT,
    quantidade_visualizacoes INT,
    quantidade_respostas INT,
    pontuacao INT,
    respondida BOOLEAN
);

CREATE TABLE IF NOT EXISTS bridge_tags (
    id_pergunta INT,
    id_tags INT
);