-- Ticket 84697 - Autor DALVES - Data 18/01/2023
-- Create table
create table ESOCIAL.TSOC_CTR_RETIFICACAO
(
  id_retificacao     NUMBER(14) not null,
  id_ctr_retificacao NUMBER(14) not null,
  cod_ins            NUMBER not null,
  id_apuracao        VARCHAR2(1),
  periodo            VARCHAR2(7),
  cpf_benef          VARCHAR2(11),
  nr_recibo          VARCHAR2(23),
  flg_status         VARCHAR2(1),
  id_evento          NUMBER(16),
  id_origem          NUMBER(16),
  dat_ing            DATE not null,
  dat_ult_atu        DATE not null,
  nom_usu_ult_atu    VARCHAR2(20) not null,
  nom_pro_ult_atu    VARCHAR2(40) not null
);
-- Add comments to the columns 
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.id_retificacao
  is 'Sequencia da retifica��o';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.id_ctr_retificacao
  is 'Identificador de controle das retifica��es deve ser unico por retifica��o';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.cod_ins
  is 'C�digo da institui��o';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.id_apuracao
  is 'Tipo de apura��o "T" - Decimo Terceiro "N" - Folha Normal';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.periodo
  is 'Periodo de Apura��o formato (AAAA para "T") / (AAAA-MM para "N")';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.cpf_benef
  is 'CPF do Benefici�rio';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.nr_recibo
  is 'N�mero do protocolo do envio ao eSocial';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.flg_status
  is 'Flag de status "A" - Aguardando Processamento / "F" - Finalizado / "E" - Erro';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.id_evento
  is 'Identificador do Evento';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.id_origem
  is 'Identificador da Origem';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.dat_ing
  is 'Data de Ingresso da tabela ';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.dat_ult_atu
  is 'Data de Altera��o';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.nom_usu_ult_atu
  is 'Nome do usu�rio da �ltima altera��o';
comment on column ESOCIAL.TSOC_CTR_RETIFICACAO.nom_pro_ult_atu
  is 'Nome do processo da �ltima altera��o';
