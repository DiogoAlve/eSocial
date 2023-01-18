create or replace PACKAGE         pac_esocial_xml_102 AS

  gb_rec_erro esocial.tsoc_ctr_erro_processo%ROWTYPE;

  PROCEDURE sp_seta_processo(p_id_ctr_processo IN esocial.tsoc_ctr_processo.id_ctr_processo%TYPE,
                             p_nom_evento      IN VARCHAR2,
                             p_qtd_registros   IN NUMBER);

  PROCEDURE sp_xml_lote(p_id_evento IN NUMBER,
                        p_id_pk     IN NUMBER,
                        p_header    OUT CLOB,
                        p_trailer   OUT CLOB);

  PROCEDURE sp_xml_generico(p_cod_ins    IN NUMBER,
                            p_nom_evento IN VARCHAR2,
                            p_acao       IN VARCHAR2,
                            p_id         IN NUMBER);

  PROCEDURE sp_xml_1000(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_1010(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_1207(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2400_old(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2400(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2405(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2410(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2416(p_id_ctr_processo IN NUMBER);
  
  PROCEDURE sp_xml_2418(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_2420(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_xml_1210(p_id_ctr_processo in esocial.tsoc_ctr_processo.id_ctr_processo%type);

  PROCEDURE sp_xml_1210_t(p_id_ctr_processo in esocial.tsoc_ctr_processo.id_ctr_processo%type);

  PROCEDURE sp_xml_1299(p_id_ctr_processo IN NUMBER);
  
  PROCEDURE sp_xml_1298(p_id_ctr_processo IN NUMBER);

  PROCEDURE sp_arqs_qualificacao_cadastral;
  
  PROCEDURE SP_ATUALIZA_EVENTO(P_ID_PK        IN NUMBER,
                               P_TABELA       IN VARCHAR2,
                               P_XML_ASSINADO IN CLOB,
                               P_COD_INS      IN NUMBER,
                               P_TIP_ATU      IN VARCHAR2
                               );
  
  

END pac_esocial_xml_102;
