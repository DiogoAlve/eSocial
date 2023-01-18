CREATE OR REPLACE PACKAGE         pac_esocial_xml_102 AS

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

/


CREATE OR REPLACE PACKAGE BODY         pac_esocial_xml_102 IS

  g_acao         VARCHAR2(1);
  g_paisresid    VARCHAR2(3);
  g_tp_beneficio VARCHAR2(4);
  g_flg_acao     VARCHAR2(1);
  g_ocorrencia   NUMBER;

  PROCEDURE sp_carrega_ids(p_id_ctr_processo IN esocial.tsoc_ctr_processo.id_ctr_processo%TYPE) IS
    v_cod_ins NUMBER;
  BEGIN

    SELECT a.cod_ins
      INTO v_cod_ins
      FROM esocial.tsoc_ctr_processo    a,
           esocial.tsoc_par_processo    b,
           esocial.tsoc_ctr_periodo_det c,
           esocial.tsoc_ctr_periodo     d
     WHERE a.cod_ins = b.cod_ins
       AND a.id_processo = b.id_processo
       AND a.cod_ins = c.cod_ins
       AND a.id_periodo = c.id_periodo
       AND b.id_origem = c.id_origem
       AND b.cod_ins = c.cod_ins
       AND b.id_evento = c.id_evento
       AND a.id_ctr_processo = p_id_ctr_processo
       AND d.id_periodo = c.id_periodo
       AND d.cod_ins = c.cod_ins
       AND b.flg_status = 'A' --processo parametrizado como ativo
       AND a.flg_status = 'A' --processo com status p definido no inicio do programa
       AND c.flg_status IN ('A', 'R') --periodo aberto ou reaberto para o evento
       AND d.flg_status IN ('A', 'R'); --periodo aberto ou reaberto

  END sp_carrega_ids;

  FUNCTION fc_tag(p_nom_registro IN VARCHAR2,
                  p_cod_ins      IN NUMBER,
                  p_nom_evento   IN VARCHAR2,
                  p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

    vxml          VARCHAR2(100);
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

  BEGIN

    -- identifico se o parametro e para abertura de tag

    IF p_abre_fecha = 'A' THEN

      -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

      SELECT tet.nom_registro
        INTO vnom_registro
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.num_versao_evento = '1'
         AND tet.tip_elemento = 'A'
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
         AND tet.nom_registro_pai = p_nom_registro;

      vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

    ELSIF p_abre_fecha = 'F' THEN

      vxml := '</' || p_nom_registro || '>';

    END IF;

    RETURN vxml;

  EXCEPTION
    WHEN no_data_found THEN

      -- caso n?o exista atributo definido para a tag, apenas a abro
      vxml := '<' || p_nom_registro || '>';
      RETURN vxml;

    WHEN OTHERS THEN

      raise_application_error(-20001, 'Erro em fc_tag: ' || SQLERRM);

  END fc_tag;

  FUNCTION fc_tag_sub(p_nom_registro IN VARCHAR2,
                  p_cod_ins      IN NUMBER,
                  p_nom_evento   IN VARCHAR2,
                  p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

    vxml          VARCHAR2(100);
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

  BEGIN

    -- identifico se o parametro e para abertura de tag

    IF p_abre_fecha = 'A' THEN

      -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

      SELECT tet.nom_registro
        INTO vnom_registro
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.num_versao_evento = '1'
         AND tet.tip_elemento = 'A'
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
         AND tet.nom_registro_pai = p_nom_registro;

      vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

    ELSIF p_abre_fecha = 'F' THEN

      vxml := '</' || p_nom_registro || '>';

    END IF;

    RETURN vxml;

  EXCEPTION
    WHEN no_data_found THEN

      -- caso n?o exista atributo definido para a tag, apenas a abro
      --DALVES - 29/11/2021 S1010
      --vxml := '<' || p_nom_registro || '>';
      vxml := '';--'<' || p_nom_registro || '>';
      RETURN vxml;

    WHEN OTHERS THEN

      raise_application_error(-20001, 'Erro em fc_tag: ' || SQLERRM);

  END fc_tag_sub;

  FUNCTION fc_set_valor(p_nom_evento     IN VARCHAR2,
                        p_cod_ins        IN NUMBER,
                        p_xml            IN CLOB,
                        p_valor          VARCHAR2,
                        p_num_seq_coluna NUMBER) RETURN CLOB IS

    vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
    nqtd_maxima_registro tsoc_par_estruturas_xml.nom_registro_pai%TYPE;
    vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

    vxml   CLOB;
    vvalor VARCHAR2(100);

    --   raise_tam_invalido EXCEPTION;
  BEGIN

    vvalor := p_valor;

    -- antes de setar o valor no xml, valido a formatacao do campo e o seu tamanho
    -- se estao de acordo com o parametrizado

    SELECT tet.nom_registro, tet.nom_registro_pai, tet.tip_elemento
      INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
      FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
     WHERE tev.cod_ins = tet.cod_ins
       AND tev.cod_evento = tet.cod_evento
       AND tev.num_versao_evento = tet.num_versao_evento
       AND tev.cod_ins = p_cod_ins
       AND tev.nom_evento = p_nom_evento
       AND tev.num_versao_evento = '1'
       AND tev.dat_fim_vig IS NULL
       AND (tet.flg_obrigatorio = 'S' OR
           (tet.flg_obrigatorio = 'N' AND
           tet.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
       AND tet.num_seq_coluna = p_num_seq_coluna;

    /*    IF vdsc_formato IS NOT NULL THEN

      vvalor := to_char(to_date(vvalor, 'DD/MM/RRRR'), vdsc_formato);

    END IF;*/

    /*    IF length(vvalor) > nqtd_maxima_registro THEN
      RAISE raise_tam_invalido;
    ELSE*/

    -- seto o valor no xml, dentro da tag passada como parametro

    IF vtip_elemento = 'A' THEN

      vxml := substr(p_xml,
                     1,
                     (instr(p_xml, vnom_registro, 1)) +
                     length(vnom_registro)) || '"' || vvalor || '"' ||
              substr(p_xml,
                     (instr(p_xml, vnom_registro, 1)) +
                     length(vnom_registro) + 1);

    ELSE

      vxml := substr(p_xml,
                     1,
                     (instr(p_xml,
                            vnom_registro,
                            instr(p_xml, nqtd_maxima_registro))) +
                     length(vnom_registro)) || vvalor ||
              substr(p_xml,
                     (instr(p_xml,
                            vnom_registro,
                            instr(p_xml, nqtd_maxima_registro))) +
                     length(vnom_registro) + 1);

    END IF;
    RETURN vxml;
    --    END IF;

  EXCEPTION

    WHEN no_data_found THEN
      RETURN p_xml;

    /*    WHEN raise_tam_invalido THEN
    raise_application_error(-20001,
                            'Tamanho invalido para ' || vnom_registro ||
                            ' - ' || vvalor || '. Maximo ' ||
                            nqtd_maxima_registro || ' posicoes.');*/
    WHEN OTHERS THEN

      raise_application_error(-20001, 'Erro em fc_set_valor: ' || SQLERRM);

  END fc_set_valor;

  PROCEDURE sp_xml_lote(p_id_evento IN NUMBER,
                        p_id_pk     IN NUMBER,
                        p_header    OUT CLOB,
                        p_trailer   OUT CLOB) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    vcod_evento      tsoc_par_evento.cod_evento%TYPE;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

  BEGIN

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    SELECT --tsx.dsc_sql,
     tev.num_versao_xml, tev.dsc_encoding_xml, tev.num_cnpj_empregador
      INTO --vdsc_sql,
           vnum_versao_xml,
           vdsc_encoding_xml,
           vnum_cnpj_empregador
      FROM tsoc_par_eventos_xml    tev,
           tsoc_par_estruturas_xml tet,
           tsoc_par_sql_xml        tsx
     WHERE tev.cod_ins = tet.cod_ins
       AND tev.cod_evento = tet.cod_evento
       AND tev.num_versao_evento = tet.num_versao_evento
       AND tev.cod_ins = tsx.cod_ins
       AND tev.cod_evento = tsx.cod_evento
       AND tev.num_versao_evento = tsx.num_versao_evento
       AND tsx.num_seq_sql = 1
       AND tev.cod_ins = 1
       AND tev.cod_evento = 1
       AND tev.dat_fim_vig IS NULL
       AND tet.num_seq = 1
       AND tet.flg_sql = 'S'
       AND tet.flg_obrigatorio = 'S';

    --  vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);
    SELECT ev.nom_tabela, ev.cod_evento
      INTO vtab_update, vcod_evento
      FROM tsoc_par_evento ev
     WHERE ev.id_evento = p_id_evento;

    vdsc_sql := 'SELECT ev.xmlns_lote         "1",
       ev.tip_evento    "2",
       t1.tpinsc       "3",
       t1.nrinsc       "4",
       tr.tip_inscricao "5",
       tr.num_inscricao "6"
  FROM ' || vtab_update || ' t1,
       tsoc_par_transmissor    tr,
       tsoc_par_evento         ev
 WHERE ev.cod_ins = t1.cod_ins
   AND ev.cod_evento = ''' || vcod_evento || '''
   AND t1.id_pk = ' || p_id_pk;

    OPEN cur_tag FOR vdsc_sql;

    -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

    n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
    dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

    FOR x IN 1 .. cur_count LOOP

      -- percorro o cursor e defino os valores para cada coluna

      dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

    END LOOP;

    WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP

      -- variavel para controlar array de fechamento das tags
      nfechatag := 1;

      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
              vdsc_encoding_xml || '"?>' || chr(13);

      -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

      FOR c_tag IN (SELECT tet.nom_registro,
                           tet.nom_registro_pai,
                           tet.tip_elemento,
                           tet.flg_sql,
                           tet.num_seq_sql,
                           tet.num_seq_coluna
                      FROM tsoc_par_eventos_xml    tev,
                           tsoc_par_estruturas_xml tet
                     WHERE tev.cod_ins = tet.cod_ins
                       AND tev.cod_evento = tet.cod_evento
                       AND tev.num_versao_evento = tet.num_versao_evento
                       AND tev.cod_ins = 1
                       AND tev.cod_evento = 1
                       AND tet.tip_elemento IN ('G', 'CG', 'E')
                       AND tev.dat_fim_vig IS NULL
                       AND (tet.flg_obrigatorio = 'S' OR
                           (tet.flg_obrigatorio = 'N' AND
                           tet.num_seq_sql = 1))
                     ORDER BY num_seq ASC) LOOP

        -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
        IF c_tag.tip_elemento IN ('G', 'CG') THEN

          -- adiciono no array auxiliar para fechamento das tags

          afechatag(nfechatag) := c_tag.nom_registro;

          -- chamo a func de montar tag, passando parametro de abertura de tag

          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, 1, 'envioLoteEventos', 'A') ||
                  chr(13);

          nfechatag := nfechatag + 1;
        ELSE
          -- caso seja uma tag de elemento (tags que possuem valor associado)

          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, 1, 'envioLoteEventos', 'A');

          -- chamo func de montar tag com parametro de fechamento de tag
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, 1, 'envioLoteEventos', 'F') ||
                  chr(13);

        END IF;

      END LOOP;

      -- cursor para fechamento das tags de grupo

      FOR i IN REVERSE 1 .. afechatag.count LOOP

        -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
        -- onde devemos fechar a tag

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = 1
           AND tet.tip_elemento IN ('G', 'CG', 'E')
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))
           AND tev.nom_evento = 'envioLoteEventos'
           AND tet.nom_registro_pai = afechatag(i)
           AND num_seq =
               (SELECT MAX(num_seq)
                  FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
                 WHERE tev.cod_ins = tet.cod_ins
                   AND tev.cod_evento = tet.cod_evento
                   AND tev.num_versao_evento = tet.num_versao_evento
                   AND tev.cod_ins = 1
                   AND tev.nom_evento = 'envioLoteEventos'
                   AND tet.tip_elemento IN ('G', 'CG', 'E')
                   AND tev.dat_fim_vig IS NULL
                   AND (tet.flg_obrigatorio = 'S' OR
                       (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))
                   AND tet.nom_registro_pai = afechatag(i));

        -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
        cxml := substr(cxml,
                       1,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1) ||
                fc_tag(afechatag(i), 1, 'envioLoteEventos', 'F') ||
                substr(cxml,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1);

      END LOOP;

      FOR x IN 1 .. cur_count LOOP

        -- seta no xml os valores retornados pelo cursor parametrizado

        dbms_sql.column_value(n_cursor_control, x, v_valores);
        cxml := fc_set_valor('envioLoteEventos',
                             1,
                             cxml,
                             v_valores,
                             to_number(cur_desc(x).col_name));
      END LOOP;

      /*
      EXECUTE IMMEDIATE 'UPDATE ' || vtab_update || ' SET XML_ENVIO = ''' || cxml ||
                        ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' || p_id;
      COMMIT;*/

      cxml := REPLACE(cxml, '*', '');

      p_header := substr(cxml, 1, (instr(cxml, '<evento ')) - 1);

      p_trailer := substr(cxml, (instr(cxml, '</eventos>')));

    --      dbms_output.put_line(cxml);

    END LOOP;

    dbms_sql.close_cursor(n_cursor_control);

  END sp_xml_lote;

  PROCEDURE sp_gera_erro_processo IS
    v_id_cad_erro esocial.tsoc_ctr_erro_processo.id_erro%TYPE;
  BEGIN

    v_id_cad_erro := esocial.esoc_seq_id_erro_processo.nextval;

    INSERT INTO esocial.tsoc_ctr_erro_processo
      (id_erro,
       cod_ins,
       id_cad,
       nom_processo,
       id_evento,
       desc_erro,
       dat_ing,
       dat_ult_atu,
       nom_usu_ult_atu,
       nom_pro_ult_atu,
       desc_erro_bd,
       des_identificador,
       flg_tipo_erro,
       id_ctr_processo,
       det_erro)
    VALUES
      (v_id_cad_erro,
       gb_rec_erro.cod_ins,
       gb_rec_erro.id_cad,
       gb_rec_erro.nom_processo,
       gb_rec_erro.id_evento,
       gb_rec_erro.desc_erro,
       SYSDATE,
       SYSDATE,
       'ESOCIAL',
       'SP_GERA_ERRO_PROCESSO',
       gb_rec_erro.desc_erro_bd,
       gb_rec_erro.des_identificador,
       gb_rec_erro.flg_tipo_erro,
       gb_rec_erro.id_ctr_processo,
       gb_rec_erro.det_erro);

    COMMIT;

  END sp_gera_erro_processo;

  PROCEDURE sp_seta_processo(p_id_ctr_processo IN esocial.tsoc_ctr_processo.id_ctr_processo%TYPE,
                             p_nom_evento      IN VARCHAR2,
                             p_qtd_registros   IN NUMBER) IS
  BEGIN

    IF p_nom_evento = 'INICIO_PROCESSAMENTO' THEN

      UPDATE esocial.tsoc_ctr_processo
         SET dat_inicio      = SYSDATE,
             dat_fim         = NULL,
             flg_status      = 'P',
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;

      COMMIT;

    ELSIF p_nom_evento = 'FIM_PROCESSAMENTO' THEN

      UPDATE esocial.tsoc_ctr_processo
         SET dat_fim         = SYSDATE,
             flg_status      = 'F',
             qtd_registros   = p_qtd_registros,
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;

      COMMIT;

    ELSIF p_nom_evento = 'ATUALIZA_QUANTIDADE' THEN

      --ATUALIZACAO DE QUANTIDADE DE REGISTROS
      UPDATE esocial.tsoc_ctr_processo
         SET qtd_registros   = p_qtd_registros,
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;

      COMMIT;

    ELSE

      --ERRO NO PROCESSAMENTO
      UPDATE esocial.tsoc_ctr_processo
         SET flg_status      = 'E',
             dat_ult_atu     = SYSDATE,
             nom_usu_ult_atu = 'ESOCIAL',
             nom_pro_ult_atu = 'SP_SETA_PROCESSO'
       WHERE id_ctr_processo = p_id_ctr_processo;

      COMMIT;

    END IF;

  END sp_seta_processo;

  PROCEDURE sp_xml_generico(p_cod_ins    IN NUMBER,
                            p_nom_evento IN VARCHAR2,
                            p_acao       IN VARCHAR2,
                            p_id         IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vsysdate      VARCHAR2(14) := to_char(SYSDATE, 'RRRRMMDDHH24MISS');
    --    vid           VARCHAR2(36);
    --    nid           NUMBER := 1;
    vdata_ini DATE;
    vdata_fim DATE;
    --    vcod_rubr     NUMBER;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

  BEGIN
    g_acao := upper(p_acao);

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    SELECT tsx.dsc_sql,
           tev.num_versao_xml,
           tev.dsc_encoding_xml,
           tev.num_cnpj_empregador
      INTO vdsc_sql,
           vnum_versao_xml,
           vdsc_encoding_xml,
           vnum_cnpj_empregador
      FROM tsoc_par_eventos_xml    tev,
           tsoc_par_estruturas_xml tet,
           tsoc_par_sql_xml        tsx
     WHERE tev.cod_ins = tet.cod_ins
       AND tev.cod_evento = tet.cod_evento
       AND tev.num_versao_evento = tet.num_versao_evento
       AND tev.cod_ins = tsx.cod_ins
       AND tev.cod_evento = tsx.cod_evento
       AND tev.num_versao_evento = tsx.num_versao_evento
       AND tsx.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)
       AND tev.cod_ins = p_cod_ins
       AND tev.nom_evento = p_nom_evento
       AND tev.dat_fim_vig IS NULL
       AND tet.num_seq = 1
       AND tet.flg_sql = 'S'
       AND tet.flg_obrigatorio = 'S';

    --  vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

    --    vdsc_sql := vdsc_sql || ' WHERE id_pk = ' || p_id;

    OPEN cur_tag FOR vdsc_sql;

    -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

    n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
    dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

    FOR x IN 1 .. cur_count LOOP

      -- percorro o cursor e defino os valores para cada coluna

      dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

    END LOOP;

    WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP

      vdata_ini := SYSDATE;
      -- variavel para controlar array de fechamento das tags
      nfechatag := 1;

      /*      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
      vdsc_encoding_xml || '"?>' || chr(13);*/

      -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

      FOR c_tag IN (SELECT tet.nom_registro,
                           tet.nom_registro_pai,
                           tet.tip_elemento,
                           tet.flg_sql,
                           tet.num_seq_sql,
                           tet.num_seq_coluna
                      FROM tsoc_par_eventos_xml    tev,
                           tsoc_par_estruturas_xml tet
                     WHERE tev.cod_ins = tet.cod_ins
                       AND tev.cod_evento = tet.cod_evento
                       AND tev.num_versao_evento = tet.num_versao_evento
                       AND tev.cod_ins = p_cod_ins
                       AND tev.nom_evento = p_nom_evento
                       AND tet.tip_elemento IN ('G', 'CG', 'E')
                       AND tev.dat_fim_vig IS NULL
                       AND (tet.flg_obrigatorio = 'S' OR
                           (tet.flg_obrigatorio = 'N' AND
                           tet.num_seq_sql =
                           decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
                     ORDER BY num_seq ASC) LOOP

        -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
        IF c_tag.tip_elemento IN ('G', 'CG') THEN

          -- adiciono no array auxiliar para fechamento das tags

          afechatag(nfechatag) := c_tag.nom_registro;

          -- chamo a func de montar tag, passando parametro de abertura de tag

          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, p_cod_ins, p_nom_evento, 'A') ||
                  chr(13);

          nfechatag := nfechatag + 1;
        ELSE
          -- caso seja uma tag de elemento (tags que possuem valor associado)

          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, p_cod_ins, p_nom_evento, 'A');

          -- chamo func de montar tag com parametro de fechamento de tag
          cxml := cxml ||
                  fc_tag(c_tag.nom_registro, p_cod_ins, p_nom_evento, 'F') ||
                  chr(13);

        END IF;

      END LOOP;

      -- defino o valor a ser setado no atributo ID do xml

      --      vid := 'ID1' || vnum_cnpj_empregador || vsysdate || lpad(nid, 5, '0');

      --      cxml := fc_set_id(cxml, vid);

      -- sequencial do id
      --      nid := nid + 1;

      -- cursor para fechamento das tags de grupo

      FOR i IN REVERSE 1 .. afechatag.count LOOP

        -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
        -- onde devemos fechar a tag

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tet.tip_elemento IN ('G', 'CG', 'E')
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql =
               decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
           AND tev.nom_evento = p_nom_evento
           AND tet.nom_registro_pai = afechatag(i)
           AND num_seq =
               (SELECT MAX(num_seq)
                  FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
                 WHERE tev.cod_ins = tet.cod_ins
                   AND tev.cod_evento = tet.cod_evento
                   AND tev.num_versao_evento = tet.num_versao_evento
                   AND tev.cod_ins = p_cod_ins
                   AND tev.nom_evento = p_nom_evento
                   AND tet.tip_elemento IN ('G', 'CG', 'E')
                   AND tev.dat_fim_vig IS NULL
                   AND (tet.flg_obrigatorio = 'S' OR
                       (tet.flg_obrigatorio = 'N' AND
                       tet.num_seq_sql =
                       decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
                   AND tet.nom_registro_pai = afechatag(i));

        -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
        cxml := substr(cxml,
                       1,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1) ||
                fc_tag(afechatag(i), p_cod_ins, p_nom_evento, 'F') ||
                substr(cxml,
                       (instr(cxml, vnom_registro, -1)) +
                       length(vnom_registro) + 1);

      END LOOP;

      FOR x IN 1 .. cur_count LOOP

        -- seta no xml os valores retornados pelo cursor parametrizado

        dbms_sql.column_value(n_cursor_control, x, v_valores);
        cxml := fc_set_valor(p_nom_evento,
                             p_cod_ins,
                             cxml,
                             v_valores,
                             to_number(cur_desc(x).col_name));

      /*        IF x = 8 THEN

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        vcod_rubr := v_valores;

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      END IF;*/

      END LOOP;

      vdata_fim := SYSDATE;

      /*      UPDATE tsoc_1010_rubrica r
        SET r.xml_envio        = cxml,
            r.ctr_dat_ini_proc = vdata_ini,
            r.ctr_dat_fim_proc = vdata_fim
      WHERE r.rowid = v_valores;*/

      /*      INSERT INTO tsoc_analise_tempo_geracao atg
        (cod_rubrica, xml_proc, dat_ini_proc, dat_fim_proc)
      VALUES
        (vcod_rubr, cxml, vdata_ini, vdata_fim);

      COMMIT;*/

      EXECUTE IMMEDIATE 'UPDATE ' || vtab_update || ' SET XML_ENVIO = ''' || cxml ||
                        ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' || p_id;
      COMMIT;

      dbms_output.put_line(cxml);

    END LOOP;

    dbms_sql.close_cursor(n_cursor_control);

    /* EXCEPTION
    WHEN OTHERS THEN

      raise_application_error(-20001,
                              'Erro em sp_xml_generico: ' || SQLERRM);*/

  END sp_xml_generico;

  PROCEDURE sp_xml_1000(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --    vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_1000(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.num_versao_evento = '1'
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_flg_acao, 'I', 1, 'A', 2, 'E', 3)))

         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_1000';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_1000';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_1000;

    FUNCTION fc_tag_1000(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tev.num_versao_evento = '1'
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql =
               decode(g_flg_acao, 'I', 1, 'A', 2, 'E', 3)))

           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_1000';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_1000';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_1000;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_1000';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk, ctr_flg_acao
                FROM tsoc_1000_empregador
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX'
                 AND flg_vigencia = 'A') LOOP

      g_flg_acao := x.ctr_flg_acao;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = decode(g_flg_acao, 'I', 1, 'A', 2, 'E', 3)

         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtInfoEmpregador'
         AND tev.num_versao_evento = '1'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 1000;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtInfoEmpregador'
                           AND tev.num_versao_evento = '1'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql =
                               decode(g_flg_acao, 'I', 1, 'A', 2, 'E', 3)))

                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_1000(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtInfoEmpregador',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_1000(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtInfoEmpregador',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_1000(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtInfoEmpregador',
                                          'F') || chr(13);
                                         

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND
                     tet.num_seq_sql =
                     decode(g_flg_acao, 'I', 1, 'A', 2, 'E', 3)))

                 AND tev.nom_evento = 'evtInfoEmpregador'
                 AND tev.num_versao_evento = '1'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtInfoEmpregador'
                         AND tev.num_versao_evento = '1'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql =
                             decode(g_flg_acao, 'I', 1, 'A', 2, 'E', 3)))

                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_1000';
                gb_rec_erro.id_evento         := 1;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 1000';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            /* dalves 01/07/2021
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_1000(afechatag(i),
                                v_cod_ins,
                                'evtInfoEmpregador',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);*/
                           
              cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, instr(cxml, afechatag(i), 1),2)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_1000(afechatag(i),
                                v_cod_ins,
                                'evtInfoEmpregador',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, instr(cxml, afechatag(i), 1),2)) +
                           length(vnom_registro) + 1);           

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);

            /*IF x != 1 THEN
              v_valores := PAC_ESOCIAL_EVENTOS_NP.fc_formata_string(v_valores);
            END IF;*/

            cxml := fc_set_valor_1000('evtInfoEmpregador',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));
                                  

          END LOOP;
          
          --limpa tag
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

                        --dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_1000';
            gb_rec_erro.id_evento         := 1;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1000';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_1000';
      gb_rec_erro.id_evento         := 1;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1000';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_1000;

  PROCEDURE sp_xml_1010(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_1010';
        gb_rec_erro.id_evento         := 7;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk, ctr_flg_acao
                FROM tsoc_1010_rubrica
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX'
                 /*and id_pk = 188591*/) LOOP

      g_acao := x.ctr_flg_acao;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)

         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtTabRubrica'
         AND tev.num_versao_evento = '1'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 1010;

      --      vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtTabRubrica'
                           AND tev.num_versao_evento = '1'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql =
                               decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))

                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag(c_tag.nom_registro,
                                     v_cod_ins,
                                     'evtTabRubrica',
                                     'A') ||

                      chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag(c_tag.nom_registro,
                                     v_cod_ins,
                                     'evtTabRubrica',
                                     'A');
               
              
              --DALVES 29/11/2021 - S1010
           /*   IF fc_tag_sub(c_tag.nom_registro,
                                     v_cod_ins,
                                     'evtTabRubrica',
                                     'A') IS NOT NULL THEN      */               

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag(c_tag.nom_registro,
                                     v_cod_ins,
                                     'evtTabRubrica',
                                     'F') ||

                      chr(13);
             -- END IF;        

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND
                     tet.num_seq_sql =
                     decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))
                 AND tev.nom_evento = 'evtTabRubrica'
                 AND tev.num_versao_evento = '1'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtTabRubrica'
                         AND tev.num_versao_evento = '1'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql =
                             decode(g_acao, 'I', 1, 'A', 2, 'E', 3, 1)))

                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_1010';
                gb_rec_erro.id_evento         := 7;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 1010';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            /* DALVES 01/07/2021
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag(afechatag(i), v_cod_ins, 'evtTabRubrica', 'F') ||

                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);*/
              cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, instr(cxml, afechatag(i), 1),2)) +
                           length(vnom_registro) + 1) ||
                    fc_tag(afechatag(i),
                                v_cod_ins,
                                'evtTabRubrica',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, instr(cxml, afechatag(i), 1),2)) +
                           length(vnom_registro) + 1);                                       

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado
            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor('evtTabRubrica',
                                 v_cod_ins,
                                 cxml,
                                 v_valores,
                                 to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_1010';
            gb_rec_erro.id_evento         := 7;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1010';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;
            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_1010';
      gb_rec_erro.id_evento         := 7;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1010';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_1010;

 PROCEDURE sp_xml_1207(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;
    v_perapur        NUMBER;
    v_perapur_ant    NUMBER;


    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --    vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);
    
    V_QTD_OCORRENCIAS    NUMBER; 
    v_param_instr        number;
    vnom_registro_ant    tsoc_par_estruturas_xml.nom_registro%TYPE;

    FUNCTION fc_set_valor_1207(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND tet.flg_obrigatorio = 'S'
         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_1207';
        gb_rec_erro.id_evento         := 8;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_1207';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_1207;

    FUNCTION fc_tag_1207(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND tet.flg_obrigatorio = 'S'
           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_1207';
        gb_rec_erro.id_evento         := 8;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_1207';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_1207;

  
  BEGIN
    
    --execute immediate 'ALTER SESSION SET nls_numeric_characters=". "';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,''';

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM esocial.tsoc_ctr_processo ctr
       WHERE ctr.id_ctr_processo = p_id_ctr_processo
         AND ctr.flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_1207';
        gb_rec_erro.id_evento         := 8;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk
                FROM esocial.tsoc_1207_beneficio
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX'
                 /*and id_pk = 4411817*/) LOOP

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtBenPrRP'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT upper(nom_tabela)
        INTO vtab_update
        FROM esocial.tsoc_par_evento
       WHERE cod_evento = 1207;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtBenPrRP'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND tet.flg_obrigatorio = 'S'
                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_1207(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtBenPrRP',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_1207(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtBenPrRP',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_1207(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtBenPrRP',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_1207('evtBenPrRP',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;

          FOR dmdev IN (SELECT id_demonstrativo, idemdev, nrbeneficio, perRef
                          FROM esocial.tsoc_cpl_1207_demonstrativo
                         WHERE id_pk = x.id_pk) LOOP

            cxml := cxml || To_clob('<dmDev>' || chr(13) || '<ideDmDev>' ||
                    dmdev.idemdev || '</ideDmDev>' || chr(13) ||
                    '<nrBeneficio>' || dmdev.nrbeneficio ||
                    '</nrBeneficio>' || chr(13));

            SELECT COUNT(1)
              INTO v_perapur
              FROM esocial.tsoc_cpl_1207_orgao_unidade_n
             WHERE id_demonstrativo = dmdev.id_demonstrativo;

            IF v_perapur > 0 THEN
              cxml := cxml || To_clob('<infoPerApur>' || chr(13));
            END IF;

            FOR ideestab IN (SELECT id_unidade_n, tpinsc, nrinsc
                               FROM esocial.tsoc_cpl_1207_orgao_unidade_n
                              WHERE id_demonstrativo =
                                    dmdev.id_demonstrativo) LOOP

              cxml := cxml || To_clob('<ideEstab>' || chr(13) || '<tpInsc>' ||
                      ideestab.tpinsc || '</tpInsc>' || chr(13) ||
                      '<nrInsc>' || /*ideestab.nrinsc*/ '09041213000136'|| '</nrInsc>' ||
                      chr(13));

              --cxml := cxml || '<remunPerApur>' || chr(13);

              FOR itens IN (SELECT --codrubr, idetabrubr, vrrubr, qtdRubr, fatorRubr, indApurIR
                                   --TT 79611 - Esocial SPPREV S-1207: Retorno de Erro: Codigo 17 Fator N?o Decimal
                                   codrubr,
                                   idetabrubr,
                                    case when vrrubr >= 1 then
                                       TRIM(TO_CHAR(vrrubr, '99999999999999D00'))
                                       else
                                       trim(to_char(vrrubr,'0.00'))
                                       end as vrrubr,
                                   --TRIM(TO_CHAR(vrrubr, '99999999999999D00')) vrrubr,
                                   TRIM(TO_CHAR(qtdRubr, '999999999999D00')) qtdRubr,
                                   TRIM(TO_CHAR(fatorRubr, '99999D00')) fatorRubr,
                                   indApurIR
                              FROM esocial.tsoc_cpl_1207_rubrica_n
                             WHERE id_unidade_n = ideestab.id_unidade_n) LOOP
                cxml := cxml || To_clob('<itensRemun>' || chr(13) || '<codRubr>' ||
                        itens.codrubr || '</codRubr>' || chr(13) ||
                        '<ideTabRubr>' || itens.idetabrubr ||
                        --dalves s1207 - 23/11/2021
                        '</ideTabRubr>' || chr(13) ||  '<qtdRubr>' ||
                        itens.qtdRubr || '</qtdRubr>' ||chr(13) || '<fatorRubr>' ||
                        itens.fatorRubr || '</fatorRubr>' || chr(13) ||'<vrRubr>' ||
                        itens.vrrubr || '</vrRubr>' ||chr(13) || '<indApurIR>' ||
                        itens.indApurIR || '</indApurIR>' || chr(13) ||
                        '</itensRemun>' || chr(13));

              END LOOP;

              cxml := cxml /*|| '</remunPerApur>' || chr(13)*/ || To_clob('</ideEstab>' ||
                      chr(13) || '</infoPerApur>' || chr(13));

            END LOOP;
            --TT81340
            select count(1)
              into v_perapur_ant
              from esocial.tsoc_cpl_1207_proc_retroativo pr,
                   esocial.tsoc_cpl_1207_retroativo      r
             where pr.id_proc_retroativo = r.id_proc_retroativo
               and pr.id_demonstrativo = dmdev.id_demonstrativo;

            if v_perapur_ant > 0 then
              cxml := cxml || To_clob('<infoPerAnt>' ||  chr(13));
              
              FOR idePeriodo IN (select distinct r.perref, o.id_org_unidade_r, o.tpinsc, o.nrinsc
                                  from esocial.tsoc_cpl_1207_proc_retroativo pr,
                                       esocial.tsoc_cpl_1207_retroativo      r,
                                       esocial.tsoc_cpl_1207_orgao_unidade_r o
                                 where pr.id_proc_retroativo = r.id_proc_retroativo
                                   and r.id_retroativo = o.id_retroativo
                                   and pr.id_demonstrativo = dmdev.id_demonstrativo
                                 order by r.perref) LOOP
            
              cxml := cxml || To_clob('<idePeriodo>' || chr(13) || '<perRef>' || idePeriodo.perRef ||
                    '</perRef>' || chr(13));
              --TT81340
              /*FOR ideestab IN (select o.id_org_unidade_r, o.tpinsc, o.nrinsc
                                  from esocial.tsoc_cpl_1207_proc_retroativo pr,
                                       esocial.tsoc_cpl_1207_retroativo      r,
                                       esocial.tsoc_cpl_1207_orgao_unidade_r o
                                 where pr.id_proc_retroativo = r.id_proc_retroativo
                                   and r.id_retroativo = o.id_retroativo
                                   and pr.id_demonstrativo = dmdev.id_demonstrativo) LOOP*/

              cxml := cxml || To_clob('<ideEstab>' || chr(13) || '<tpInsc>' ||
                      idePeriodo.tpinsc || '</tpInsc>' || chr(13) ||
                      '<nrInsc>' || /*idePeriodo.nrinsc*/ '09041213000136'|| '</nrInsc>' ||
                      chr(13));

              --cxml := cxml || '<remunPerApur>' || chr(13);
            
              FOR itens IN (SELECT --codrubr, idetabrubr, vrrubr, qtdRubr, fatorRubr, indApurIR
                                   --TT 79611 - Esocial SPPREV S-1207: Retorno de Erro: Codigo 17 Fator N?o Decimal
                                   codrubr,
                                   idetabrubr,
                                   case when vrrubr >= 1 then
                                       TRIM(TO_CHAR(vrrubr, '99999999999999D00')) 
                                       else
                                       trim(to_char(vrrubr,'0.00'))
                                       end as vrrubr,
                                   --TRIM(TO_CHAR(vrrubr, '99999999999999D00')) vrrubr,
                                   TRIM(TO_CHAR(qtdRubr, '999999999999D00')) qtdRubr,
                                   TRIM(TO_CHAR(fatorRubr, '99999D00')) fatorRubr,
                                   indApurIR
                              FROM esocial.tsoc_cpl_1207_rubrica_r
                             WHERE id_org_unidade_r = idePeriodo.id_org_unidade_r
                               AND vrrubr > 0) LOOP
           
                cxml := cxml || To_clob('<itensRemun>' || chr(13) || '<codRubr>' ||
                        itens.codrubr || '</codRubr>' || chr(13) ||
                        '<ideTabRubr>' || itens.idetabrubr ||
                        --dalves s1207 - 23/11/2021
                        '</ideTabRubr>' || chr(13) ||  '<qtdRubr>' ||
                        itens.qtdRubr || '</qtdRubr>' ||chr(13) || '<fatorRubr>' ||
                        itens.fatorRubr || '</fatorRubr>' || chr(13) ||'<vrRubr>' ||
                        itens.vrrubr || '</vrRubr>' ||chr(13) || '<indApurIR>' ||
                        itens.indApurIR || '</indApurIR>' || chr(13) ||
                        '</itensRemun>' || chr(13));
   
              END LOOP;

              cxml := cxml /*|| '</remunPerApur>' || chr(13)*/ || To_clob('</ideEstab>' ||
                      chr(13));

            --END LOOP;
            --

              cxml := cxml || To_clob('</idePeriodo>' ||  chr(13));
              END LOOP;--idePeriodo
              cxml := cxml || To_clob('</infoPerAnt>' || chr(13));
            end if;--v_perapur_ant
            
            cxml := cxml || To_clob('</dmDev>' || chr(13));

          END LOOP;
          
          V_QTD_OCORRENCIAS := 0;
          vnom_registro_ant := NULL;

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR num_seq_sql = 3)
                 AND tev.nom_evento = 'evtBenPrRP'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtBenPrRP'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR num_seq_sql = 3)
                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_1207';
                gb_rec_erro.id_evento         := 8;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 1207';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
           V_QTD_OCORRENCIAS := REGEXP_COUNT(cxml,'<'||vnom_registro||'>');
            if V_QTD_OCORRENCIAS > 1 and vnom_registro_ant is null then
              v_param_instr := 1;
              vnom_registro_ant := vnom_registro;
              
              IF vnom_registro != 'dmDev' THEN 
                cxml := To_clob(substr(cxml,
                             1,
                             (instr(cxml, '</'||vnom_registro||'>', v_param_instr)) +
                             length('</'||vnom_registro||'>')) ||
                      fc_tag_1207(afechatag(i), v_cod_ins, 'evtBenPrRP', 'F') ||
                      substr(cxml,
                             (instr(cxml, '</'||vnom_registro||'>', v_param_instr)) +
                             length('</'||vnom_registro||'>')));
              ELSE
                --TT78307 - ERRO FECHAMENTO DA TAG
                  cxml := To_clob(substr(cxml,
                             1,
                             (instr(cxml, '</'||vnom_registro||'>', -1)) +
                             length('</'||vnom_registro||'>')) ||
                      fc_tag_1207(afechatag(i), v_cod_ins, 'evtBenPrRP', 'F') ||
                      substr(cxml,
                             (instr(cxml, '</'||vnom_registro||'>', -1)) +
                             length('</'||vnom_registro||'>')));
                
              END IF;             
            else
              v_param_instr := -1;
              
              cxml := To_clob(substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, v_param_instr)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_1207(afechatag(i), v_cod_ins, 'evtBenPrRP', 'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, v_param_instr)) +
                           length(vnom_registro) + 1));
            end if;  
 
            
          /*  
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, v_param_instr)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_1207(afechatag(i), v_cod_ins, 'evtBenPrRP', 'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, v_param_instr)) +
                           length(vnom_registro) + 1);*/

          END LOOP;

          cxml := To_clob(REPLACE(cxml, '*', ''));
          
           --limpa tag
          cxml := To_clob(esocial.limpa_tag(esocial.limpa_tag(cxml,1),2));

         /* EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;*/
          --Marca status AA (Aguardando Assinatura) no evento. 
          SP_ATUALIZA_EVENTO(x.id_pk,
                             vtab_update,
                             cxml,
                             1,
                             'AA');

          --              dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

         /* sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);*/
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_1207';
            gb_rec_erro.id_evento         := 8;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1207';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_1207';
      gb_rec_erro.id_evento         := 8;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1207';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_1207;

  PROCEDURE sp_xml_2400_old(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --    vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_2400(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2400';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_2400';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_2400;

    FUNCTION fc_tag_2400(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2400';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_2400';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_2400;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2400';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT b.id_pk, b.endereco_paisresid
                FROM tsoc_2400_beneficiario_ini b
               WHERE (b.id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR v_faixa_ini IS NULL)
                 AND b.ctr_flg_status = 'AX'
                 --DALVES 17/03/2022
                 --Dados pessoais (evento s-2400) deve ser unico por CPF
                 AND b.id_origem =
                     (SELECT DECODE(qtd_origem, 1, b.id_origem, 1) id_origem_ex
                        FROM (SELECT COUNT(t.id_origem) qtd_origem, t.ctr_num_cpf
                                FROM esocial.tsoc_2400_beneficiario_ini t
                               WHERE t.ctr_num_cpf = b.ctr_num_cpf
                               GROUP BY t.ctr_num_cpf))
                              ) LOOP

      g_paisresid := x.endereco_paisresid;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = decode(g_paisresid, '105', 1, 2)

         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenefIn'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 2400;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenefIn'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql =
                               decode(g_paisresid, '105', 1, 2)))

                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND
                     tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

                 AND tev.nom_evento = 'evtCdBenefIn'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenefIn'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql =
                             decode(g_paisresid, '105', 1, 2)))

                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2400';
                gb_rec_erro.id_evento         := 1;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2400';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2400(afechatag(i),
                                v_cod_ins,
                                'evtCdBenefIn',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2400('evtCdBenefIn',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --              dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2400';
            gb_rec_erro.id_evento         := 1;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2400';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2400';
      gb_rec_erro.id_evento         := 1;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2400';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_2400_old;

  --TT78361
  PROCEDURE sp_xml_2400(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --    vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);
    
    --
/*    afechatag_aux t_array;
    nfechatag_aux  NUMBER;
    cxml_aux         CLOB;*/
    --

    FUNCTION fc_set_valor_2400(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;
      vnom_registro_pai    tsoc_par_estruturas_xml.nom_registro_pai%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento, tet.nom_registro_pai
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento, vnom_registro_pai
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_paisresid, '105', 1, 2))OR 
             (lower(tet.nom_registro_pai) = 'dependente')
             )

          AND (tet.num_seq_coluna = p_num_seq_coluna or
         (lower(tet.nom_registro_pai) = 'dependente' and tet.num_seq = p_num_seq_coluna));

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1,g_ocorrencia)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1,g_ocorrencia)) +
                       length(vnom_registro) + 1);

      ELSE

        --TT79355 - 29/06/2022 - DALVES - buscar do ultima ao primeiro
        IF vnom_registro_pai = 'dependente' THEN
          vxml := substr(p_xml,
                         1,
                         (instr(p_xml, vnom_registro, 1,g_ocorrencia)) +
                         length(vnom_registro)) || vvalor ||
                  substr(p_xml,
                         (instr(p_xml, vnom_registro, 1,g_ocorrencia)) +
                         length(vnom_registro) + 1);
        ELSE
          vxml := substr(p_xml,
                         1,
                         (instr(p_xml, vnom_registro, -1,g_ocorrencia+1)) +
                         length(vnom_registro)) || vvalor ||
                  substr(p_xml,
                         (instr(p_xml, vnom_registro, -1,g_ocorrencia+1)) +
                         length(vnom_registro) + 1);
        END IF;                               

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2400';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_2400';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_2400;

    FUNCTION fc_tag_2400(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2400';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_2400';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_2400;
    --
    FUNCTION fc_dados_dep (p_id_pk esocial.tsoc_2400_beneficiario_ini.id_pk%TYPE)
    RETURN CLOB IS
    --
    vxml          CLOB := '';
    --  
    --
    vafechatag_aux t_array;
    vnfechatag_aux  NUMBER;
    vcxml_aux         CLOB;
    --
    BEGIN
      --
      g_ocorrencia := 1;
      vcxml_aux     := '';
      --
      FOR reg IN (SELECT dep.dependente_tpdep tpDep
                        ,dep.dependente_nmdep nmDep
                        ,to_char(dep.dependente_dtnascto,'RRRR-MM-DD') dtNascto
                        ,dep.dependente_cpfdep cpfDep
                        ,dep.dependente_sexodep sexoDep
                        ,dep.dependente_depirrf depIRRF
                        ,dep.dependente_incfismen incFisMen
                        ,dep.id_pk
                        ,dep.id_cad_dependente 
                    FROM  esocial.TSOC_2400_BENEFICIARIO_INI BEN
                         ,esocial.TSOC_2400_DEPENDENTE DEP
                   WHERE ben.id_pk = p_id_pk 
                     AND ben.id_cad_beneficiario = dep.id_cad_beneficiario
                 )
      LOOP
      --
      DELETE esocial.tb_dep_dados_aux;
      COMMIT;
      --
      INSERT INTO esocial.tb_dep_dados_aux VALUES (44 ,reg.tpDep    ,'tpDep');        
      INSERT INTO esocial.tb_dep_dados_aux VALUES (45 ,reg.nmDep    ,'nmDep');       
      INSERT INTO esocial.tb_dep_dados_aux VALUES (46 ,reg.dtNascto ,'dtNascto');    
      INSERT INTO esocial.tb_dep_dados_aux VALUES (47 ,reg.cpfDep   ,'cpfDep');     
      INSERT INTO esocial.tb_dep_dados_aux VALUES (48 ,reg.sexoDep  ,'sexoDep');    
      INSERT INTO esocial.tb_dep_dados_aux VALUES (49 ,reg.depIRRF  ,'depIRRF');    
      INSERT INTO esocial.tb_dep_dados_aux VALUES (50 ,reg.incFisMen,'incFisMen');    
      --  
      vafechatag_aux.delete();
      vnfechatag_aux := 1;
      --
      FOR c_tag IN (SELECT tet.nom_registro,
                           tet.nom_registro_pai,
                           tet.tip_elemento,
                           tet.flg_sql,
                           tet.num_seq_sql,
                           tet.num_seq_coluna
                      FROM esocial.tsoc_par_eventos_xml    tev,
                           esocial.tsoc_par_estruturas_xml tet
                     WHERE tev.cod_ins = tet.cod_ins
                       AND tev.cod_evento = tet.cod_evento
                       AND tev.num_versao_evento = tet.num_versao_evento
                       AND tev.cod_ins = v_cod_ins
                       AND tev.nom_evento = 'evtCdBenefIn'
                       AND tet.tip_elemento IN ('G', 'CG', 'E')
                       AND tev.dat_fim_vig IS NULL
                       AND (LOWER(tet.nom_registro_pai) = 'dependente' OR LOWER(tet.nom_registro) = 'dependente')
                       ORDER BY num_seq ASC
                   ) 
      LOOP
        -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
        IF c_tag.tip_elemento IN ('G', 'CG') THEN
          -- adiciono no array auxiliar para fechamento das tags
          vafechatag_aux(vnfechatag_aux) := c_tag.nom_registro;
          -- chamo a func de montar tag, passando parametro de abertura de tag
          vcxml_aux := vcxml_aux || fc_tag_2400(c_tag.nom_registro,
                                      v_cod_ins,
                                      'evtCdBenefIn',
                                      'A') || chr(13);
          vnfechatag_aux := vnfechatag_aux + 1;
          --
        ELSE
          -- caso seja uma tag de elemento (tags que possuem valor associado)
          vcxml_aux := vcxml_aux || fc_tag_2400(c_tag.nom_registro,
                                      v_cod_ins,
                                      'evtCdBenefIn',
                                      'A');
          -- chamo func de montar tag com parametro de fechamento de tag
          vcxml_aux := vcxml_aux || fc_tag_2400(c_tag.nom_registro,
                                      v_cod_ins,
                                      'evtCdBenefIn',
                                      'F') || chr(13);
          --
        END IF;
        --
      END LOOP;
      -- cursor para fechamento das tags de grupo
      FOR i IN REVERSE 1 .. vafechatag_aux.count
      LOOP
        --
        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
          WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = v_cod_ins
           AND tev.nom_evento = 'evtCdBenefIn'
           AND tet.tip_elemento IN ('G', 'CG', 'E')
           AND tev.dat_fim_vig IS NULL
           AND (LOWER(tet.nom_registro_pai) = 'dependente' OR LOWER(tet.nom_registro) = 'dependente')
           AND tet.nom_registro_pai = vafechatag_aux(i)
           AND num_seq =
               (SELECT MAX(num_seq)
                  FROM tsoc_par_eventos_xml    tev,
                       tsoc_par_estruturas_xml tet
                 WHERE tev.cod_ins = tet.cod_ins
                     AND tev.cod_evento = tet.cod_evento
                     AND tev.num_versao_evento = tet.num_versao_evento
                     AND tev.cod_ins = v_cod_ins
                     AND tev.nom_evento = 'evtCdBenefIn'
                     AND tet.tip_elemento IN ('G', 'CG', 'E')
                     AND tev.dat_fim_vig IS NULL
                     AND (LOWER(tet.nom_registro_pai) = 'dependente' OR LOWER(tet.nom_registro) = 'dependente')
                     AND tet.nom_registro_pai = vafechatag_aux(i));
        --
        vcxml_aux := substr(vcxml_aux,
                       1,
                       (instr(vcxml_aux, vnom_registro, -1)) +
                       length(vnom_registro) + 1) ||
                fc_tag_2400(vafechatag_aux(i),
                            v_cod_ins,
                            'evtCdBenefIn',
                            'F') ||
                substr(vcxml_aux,
                       (instr(vcxml_aux, vnom_registro, -1)) +
                       length(vnom_registro) + 1);
        --
      END LOOP;
      --
      FOR t IN (SELECT * 
                  FROM esocial.tb_dep_dados_aux
                ORDER BY num_seq  
               )
      LOOP
        -- seta no xml os valores retornados pelo cursor parametrizado
        
        vcxml_aux := fc_set_valor_2400('evtCdBenefIn',
                                      v_cod_ins,
                                      vcxml_aux,
                                      t.valor,
                                      t.num_seq);
        --
      END LOOP;
      --
      g_ocorrencia := g_ocorrencia + 2 ;
      --
      
      --Marca status AA (Aguardando Assinatura) no evento. 
    /*  SP_ATUALIZA_EVENTO(reg.id_pk,
                         'TSOC_2400_DEPENDENTE',
                         vcxml_aux,
                         1,
                         'AA'
                         );*/
    END LOOP;
    --
    vxml := vcxml_aux;
    --
  RETURN vxml;
  --             
  END fc_dados_dep;
  --
  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2400';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT b.id_pk, b.endereco_paisresid
                FROM tsoc_2400_beneficiario_ini b
               WHERE (b.id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR v_faixa_ini IS NULL)
                 AND b.ctr_flg_status = 'AX'
                 --restricao
                 AND rownum <= 50000
                -- and b.id_cad_beneficiario in (1194509,1194523) 
                 --DALVES 17/03/2022
                 --Dados pessoais (evento s-2400) deve ser unico por CPF
                 AND b.id_origem =
                     (SELECT DECODE(qtd_origem, 1, b.id_origem, 1) id_origem_ex
                        FROM (SELECT COUNT(t.id_origem) qtd_origem, t.ctr_num_cpf
                                FROM esocial.tsoc_2400_beneficiario_ini t
                               WHERE t.ctr_num_cpf = b.ctr_num_cpf
                               GROUP BY t.ctr_num_cpf))) LOOP

      g_paisresid := x.endereco_paisresid;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = decode(g_paisresid, '105', 1, 2)

         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenefIn'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 2400;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);
      
     
      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);
        --dbms_output.put_line (x||v_valores);
        --dbms_output.put_line (v_valores) ;

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenefIn'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql =
                               decode(g_paisresid, '105', 1, 2)))

                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'A') || chr(13);
              --dbms_output.put_line (c_tag.nom_registro || c_tag.tip_elemento);
              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'A');
              --dbms_output.put_line (c_tag.nom_registro||c_tag.tip_elemento);
              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2400(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefIn',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND
                     tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

                 AND tev.nom_evento = 'evtCdBenefIn'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenefIn'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql =
                             decode(g_paisresid, '105', 1, 2)))

                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                vnom_registro := afechatag(i);
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2400';
                gb_rec_erro.id_evento         := 1;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2400';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2400(afechatag(i),
                                v_cod_ins,
                                'evtCdBenefIn',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          --dbms_output.put_line (vnom_registro || 'FECHA');
          END LOOP;
          --dbms_output.put_line (cxml);
          --
          g_ocorrencia := 1;
          --
          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            --dbms_output.put_line (x||v_valores);
            cxml := fc_set_valor_2400('evtCdBenefIn',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          --INSERT INTO user_ipesp.fe_confere VALUES (cxml, SYSDATE);
          -- popula as informacoes de dependente
          --cxml:= REPLACE (cxml,'<dependente>'||CHR(13)||'</dependente>',fc_dados_dep(x.id_pk));
          cxml := esocial.replace_clob(cxml,'<dependente>'||CHR(13)||'</dependente>',fc_dados_dep(x.id_pk));

          --limpa tag
          --INSERT INTO user_ipesp.fe_confere VALUES (cxml, SYSDATE);
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          /*EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AX'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;*/
          
          --Marca status AA (Aguardando Assinatura) no evento. 
          SP_ATUALIZA_EVENTO(x.id_pk,
                             vtab_update,
                             cxml,
                             1,
                             'AA');
          g_ocorrencia   := '';
          --              dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2400';
            gb_rec_erro.id_evento         := 1;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2400';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2400';
      gb_rec_erro.id_evento         := 1;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2400';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_2400;


  PROCEDURE sp_xml_2405(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    -- vdata_ini     DATE;
    -- vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_2405(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND
             tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2405';
        gb_rec_erro.id_evento         := 2;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_2405';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_2405;

    FUNCTION fc_tag_2405(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND
               tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2405';
        gb_rec_erro.id_evento         := 2;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_2405';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_2405;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2405';
        gb_rec_erro.id_evento         := 2;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk, endereco_paisresid
                FROM tsoc_2405_beneficiario_alt
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP

      g_paisresid := x.endereco_paisresid;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = decode(g_paisresid, '105', 1, 2)

         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenefAlt'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 2405;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenefAlt'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql =
                               decode(g_paisresid, '105', 1, 2)))

                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_2405(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefAlt',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_2405(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefAlt',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2405(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenefAlt',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND
                     tet.num_seq_sql = decode(g_paisresid, '105', 1, 2)))

                 AND tev.nom_evento = 'evtCdBenefAlt'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenefAlt'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql =
                             decode(g_paisresid, '105', 1, 2)))

                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2405';
                gb_rec_erro.id_evento         := 2;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2405';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2405(afechatag(i),
                                v_cod_ins,
                                'evtCdBenefAlt',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2405('evtCdBenefAlt',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          --dalves 09/02/2022
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --      dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2405';
            gb_rec_erro.id_evento         := 2;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2405';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2405';
      gb_rec_erro.id_evento         := 2;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2405';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_2405;

  PROCEDURE sp_xml_2410(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --  vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_2410(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END))
         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2410';
        gb_rec_erro.id_evento         := 3;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_2410';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_2410;

    FUNCTION fc_tag_2410(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN g_tp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2410';
        gb_rec_erro.id_evento         := 3;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_2410';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_2410;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2410';
        gb_rec_erro.id_evento         := 3;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk, dadosbeneficio_tpbeneficio
                FROM tsoc_2410_beneficio_ini
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX'
                 /*and id_pk = 1014006*/) LOOP

      g_tp_beneficio := x.dadosbeneficio_tpbeneficio;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenIn'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 2410;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet. tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenIn'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                                 WHEN g_tp_beneficio IN
                                      (SELECT DISTINCT cod_esocial
                                         FROM esocial.tsoc_par_sigeprev_esocial cse
                                        WHERE cse.cod_tipo = 7
                                          AND upper(des_esocial) LIKE
                                              '%PENS_O%MORTE%') THEN
                                  2
                                 ELSE
                                  1
                               END))
                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_2410(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenIn',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_2410(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenIn',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2410(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenIn',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                       WHEN g_tp_beneficio IN
                            (SELECT DISTINCT cod_esocial
                               FROM esocial.tsoc_par_sigeprev_esocial cse
                              WHERE cse.cod_tipo = 7
                                AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                        2
                       ELSE
                        1
                     END))
                 AND tev.nom_evento = 'evtCdBenIn'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenIn'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                               WHEN g_tp_beneficio IN
                                    (SELECT DISTINCT cod_esocial
                                       FROM esocial.tsoc_par_sigeprev_esocial cse
                                      WHERE cse.cod_tipo = 7
                                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                                2
                               ELSE
                                1
                             END))
                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2410';
                gb_rec_erro.id_evento         := 3;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2410';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2410(afechatag(i), v_cod_ins, 'evtCdBenIn', 'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2410('evtCdBenIn',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          --dalves 09/02/2022
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);
          cxml := esocial.limpa_tag(cxml,2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --  dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2410';
            gb_rec_erro.id_evento         := 3;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2410';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2410';
      gb_rec_erro.id_evento         := 3;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2410';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_2410;

  PROCEDURE sp_xml_2416(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    -- vdata_ini     DATE;
    -- vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_2416(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END))
         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2416';
        gb_rec_erro.id_evento         := 4;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_2416';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_2416;

    FUNCTION fc_tag_2416(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN g_tp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2416';
        gb_rec_erro.id_evento         := 4;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_2416';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_2416;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2416';
        gb_rec_erro.id_evento         := 4;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk, dadosbeneficio_tpbeneficio
                FROM tsoc_2416_beneficio_alt
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP

      g_tp_beneficio := x.dadosbeneficio_tpbeneficio;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenAlt'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 2416;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenAlt'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                                 WHEN g_tp_beneficio IN
                                      (SELECT DISTINCT cod_esocial
                                         FROM esocial.tsoc_par_sigeprev_esocial cse
                                        WHERE cse.cod_tipo = 7
                                          AND upper(des_esocial) LIKE
                                              '%PENS_O%MORTE%') THEN
                                  2
                                 ELSE
                                  1
                               END))
                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_2416(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenAlt',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_2416(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenAlt',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2416(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenAlt',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                       WHEN g_tp_beneficio IN
                            (SELECT DISTINCT cod_esocial
                               FROM esocial.tsoc_par_sigeprev_esocial cse
                              WHERE cse.cod_tipo = 7
                                AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                        2
                       ELSE
                        1
                     END))
                 AND tev.nom_evento = 'evtCdBenAlt'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenAlt'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                               WHEN g_tp_beneficio IN
                                    (SELECT DISTINCT cod_esocial
                                       FROM esocial.tsoc_par_sigeprev_esocial cse
                                      WHERE cse.cod_tipo = 7
                                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                                2
                               ELSE
                                1
                             END))
                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2416';
                gb_rec_erro.id_evento         := 4;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2416';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2416(afechatag(i), v_cod_ins, 'evtCdBenAlt', 'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2416('evtCdBenAlt',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          --dalves 09/02/2022
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --  dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2416';
            gb_rec_erro.id_evento         := 4;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2416';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2416';
      gb_rec_erro.id_evento         := 4;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2416';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_2416;
  
  PROCEDURE sp_xml_2418(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_2418(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END))
         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2418';
        gb_rec_erro.id_evento         := 11;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_2418';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_2418;

    FUNCTION fc_tag_2418(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN g_tp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2418';
        gb_rec_erro.id_evento         := 11;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_2418';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_2418;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2418';
        gb_rec_erro.id_evento         := 11;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk
                FROM tsoc_2418_beneficio_reativacao
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP

      g_tp_beneficio := 1;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtReativBen'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 2418;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtReativBen'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                                 WHEN g_tp_beneficio IN
                                      (SELECT DISTINCT cod_esocial
                                         FROM esocial.tsoc_par_sigeprev_esocial cse
                                        WHERE cse.cod_tipo = 7
                                          AND upper(des_esocial) LIKE
                                              '%PENS_O%MORTE%') THEN
                                  2
                                 ELSE
                                  1
                               END))
                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_2418(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtReativBen',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_2418(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtReativBen',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2418(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtReativBen',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                       WHEN g_tp_beneficio IN
                            (SELECT DISTINCT cod_esocial
                               FROM esocial.tsoc_par_sigeprev_esocial cse
                              WHERE cse.cod_tipo = 7
                                AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                        2
                       ELSE
                        1
                     END))
                 AND tev.nom_evento = 'evtReativBen'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtReativBen'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                               WHEN g_tp_beneficio IN
                                    (SELECT DISTINCT cod_esocial
                                       FROM esocial.tsoc_par_sigeprev_esocial cse
                                      WHERE cse.cod_tipo = 7
                                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                                2
                               ELSE
                                1
                             END))
                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2418';
                gb_rec_erro.id_evento         := 11;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2418';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2418(afechatag(i),
                                v_cod_ins,
                                'evtReativBen',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2418('evtReativBen',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          --dalves 09/02/2022
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --  dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2418';
            gb_rec_erro.id_evento         := 11;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2418';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2418';
      gb_rec_erro.id_evento         := 11;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2418';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_2418;


  PROCEDURE sp_xml_2420(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_2420(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END))
         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_2420';
        gb_rec_erro.id_evento         := 5;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_2420';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_2420;

    FUNCTION fc_tag_2420(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                 WHEN g_tp_beneficio IN
                      (SELECT DISTINCT cod_esocial
                         FROM esocial.tsoc_par_sigeprev_esocial cse
                        WHERE cse.cod_tipo = 7
                          AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                  2
                 ELSE
                  1
               END))
           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_2420';
        gb_rec_erro.id_evento         := 5;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_2420';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_2420;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_2416';
        gb_rec_erro.id_evento         := 5;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk
                FROM tsoc_2420_beneficio_termino
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX') LOOP

      g_tp_beneficio := 1;

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = CASE
               WHEN g_tp_beneficio IN
                    (SELECT DISTINCT cod_esocial
                       FROM esocial.tsoc_par_sigeprev_esocial cse
                      WHERE cse.cod_tipo = 7
                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                2
               ELSE
                1
             END
         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtCdBenTerm'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 2420;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*      cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtCdBenTerm'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                                 WHEN g_tp_beneficio IN
                                      (SELECT DISTINCT cod_esocial
                                         FROM esocial.tsoc_par_sigeprev_esocial cse
                                        WHERE cse.cod_tipo = 7
                                          AND upper(des_esocial) LIKE
                                              '%PENS_O%MORTE%') THEN
                                  2
                                 ELSE
                                  1
                               END))
                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_2420(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenTerm',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_2420(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenTerm',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_2420(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtCdBenTerm',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                       WHEN g_tp_beneficio IN
                            (SELECT DISTINCT cod_esocial
                               FROM esocial.tsoc_par_sigeprev_esocial cse
                              WHERE cse.cod_tipo = 7
                                AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                        2
                       ELSE
                        1
                     END))
                 AND tev.nom_evento = 'evtCdBenTerm'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtCdBenTerm'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = CASE
                               WHEN g_tp_beneficio IN
                                    (SELECT DISTINCT cod_esocial
                                       FROM esocial.tsoc_par_sigeprev_esocial cse
                                      WHERE cse.cod_tipo = 7
                                        AND upper(des_esocial) LIKE '%PENS_O%MORTE%') THEN
                                2
                               ELSE
                                1
                             END))
                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_2420';
                gb_rec_erro.id_evento         := 5;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 2420';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_2420(afechatag(i),
                                v_cod_ins,
                                'evtCdBenTerm',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);
            cxml := fc_set_valor_2420('evtCdBenTerm',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          --dalves 09/02/2022
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);

          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --  dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_2420';
            gb_rec_erro.id_evento         := 5;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2420';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_2420';
      gb_rec_erro.id_evento         := 5;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 2420';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_2420;

  PROCEDURE sp_xml_1210(p_id_ctr_processo IN esocial.tsoc_ctr_processo.id_ctr_processo%TYPE) IS
    v_xml CLOB;
    ex_param_proc EXCEPTION;
    v_id_pk          esocial.tsoc_1210_pag_rendimentos.id_pk%TYPE;
    v_id_pgto        esocial.tsoc_cpl_1210_info_pgto.id_pgto%TYPE;
    --v_id_pgto_ben_pr esocial.tsoc_cpl_1210_det_pgto_ben_pr.id_pgto_ben_pr%TYPE;
    v_qtd_reg        NUMBER := 0;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;
  BEGIN

    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,''';
    
    --Valida Processo
    BEGIN
      sp_carrega_ids(p_id_ctr_processo);
    EXCEPTION
      WHEN OTHERS THEN
        RAISE ex_param_proc;
    END;

    sp_seta_processo(p_id_ctr_processo, 'INICIO_PROCESSAMENTO', NULL);

    SELECT ctr.faixa_ini, ctr.faixa_fim
      INTO v_faixa_ini, v_faixa_fim
      FROM tsoc_ctr_processo ctr
     WHERE id_ctr_processo = p_id_ctr_processo;

    FOR c_1210_pag_rend IN (SELECT t.*, e.xmlns
                              FROM esocial.tsoc_1210_pag_rendimentos t,
                                   esocial.tsoc_par_evento           e
                             WHERE t.ctr_flg_status = 'AX'
                               AND (t.id_pk BETWEEN v_faixa_ini AND
                                   v_faixa_fim OR (v_faixa_ini IS NULL AND
                                   v_faixa_fim IS NULL))
                               AND t.flg_vigencia = 'A'
                                  --1210
                               AND e.id_evento = 9) LOOP

      BEGIN
        v_xml :=  --eSocial------------------------------------------------------------------------------------------
         '<eSocial xmlns="' || c_1210_pag_rend.xmlns || '">' ||
                 chr(10) ||
                --evtPgtos-----------------------------------------------------------------------------------------
                 '<evtPgtos Id ="' || c_1210_pag_rend.id || '">' || chr(10) ||
                --ideEvento----------------------------------------------------------------------------------------
                 '<ideEvento>' || chr(10) || '<indRetif>' ||
                 c_1210_pag_rend.indretif || '</indRetif>' || chr(10);
        IF c_1210_pag_rend.nrrecibo IS NOT NULL THEN
          v_xml := v_xml || '<nrRecibo>' || c_1210_pag_rend.nrrecibo ||
                   '</nrRecibo>' || chr(10);
        END IF;
        v_xml := v_xml || /* dalves - s1210 - 18/11/2021
        '<indApuracao>' || c_1210_pag_rend.indapuracao ||
                 '</indApuracao>' || chr(10) ||*/ '<perApur>' ||
                 c_1210_pag_rend.perapur || '</perApur>' || chr(10) ||
                 '<tpAmb>' || c_1210_pag_rend.tpamb || '</tpAmb>' ||
                 chr(10) || '<procEmi>' || c_1210_pag_rend.procemi ||
                 '</procEmi>' || chr(10) || '<verProc>' ||
                 c_1210_pag_rend.verproc || '</verProc>' || chr(10) ||
                 '</ideEvento>' || chr(10);
        v_xml := v_xml ||
                --ideEmpregador------------------------------------------------------------------------------------
                 '<ideEmpregador>' || chr(10) || '<tpInsc>' ||
                 c_1210_pag_rend.tpinsc || '</tpInsc>' || chr(10) ||
                 '<nrInsc>' || c_1210_pag_rend.nrinsc || '</nrInsc>' ||
                 chr(10) || '</ideEmpregador>' || chr(10) ||
                --ideBenef-----------------------------------------------------------------------------------------
                 '<ideBenef>' || chr(10) || '<cpfBenef>' ||
                 c_1210_pag_rend.cpfbenef || '</cpfBenef>' || chr(10);
        --deps---------------------------------------------------------------------------------------------
       /* dalves - s1210 - 18/11/2021
        IF c_1210_pag_rend.vrdeddep IS NOT NULL THEN
          v_xml := v_xml || '<deps>' || chr(10) || '<vrDedDep>' ||
                   c_1210_pag_rend.vrdeddep || '</vrDedDep>' || chr(10) ||
                   '</deps>' || chr(10);
        END IF;*/

        v_id_pk := c_1210_pag_rend.id_pk;

        --Cursor InfoPgto
        FOR c_1210_info_pag IN (SELECT i.id_pgto,
                                       i.id_pk,
                                       to_char(i.dtpgto,'RRRR-MM-DD') as dtpgto,
                                       i.tppgto,
                                       i.indresbr,
                                       pr.id_pgto_ben_pr,
                                       pr.perref,
                                       pr.idedmdev,
                                       pr.indpgtott,
                                       to_char(TRUNC(pr.vrliq, 2),'FM999999999999.90') as vrliq
                                  FROM esocial.tsoc_cpl_1210_info_pgto       i,
                                       esocial.tsoc_cpl_1210_det_pgto_ben_pr pr
                                 WHERE i.id_pk = v_id_pk
                                   and i.id_pgto = pr.id_pgto) LOOP

          v_id_pgto := c_1210_info_pag.id_pgto;

          v_xml := v_xml ||
                  --infoPgto------------------------------------------------------------------------------------------
                   '<infoPgto>' || chr(10) || '<dtPgto>' ||
                   c_1210_info_pag.dtpgto || '</dtPgto>' || chr(10) ||
                   '<tpPgto>' || c_1210_info_pag.tppgto || '</tpPgto>' ||
                   chr(10); /* dalves - s1210 18/11/2021
                   '<indResBr>' || c_1210_info_pag.indresbr ||
                   '</indResBr>' || chr(10) ||*/ 


            v_xml := v_xml ||
                    --detPgtoBenPr------------------------------------------------------------------------------------------
                     '<perRef>' ||
                     c_1210_info_pag.perref || '</perRef>' || chr(10) ||
                     '<ideDmDev>' || c_1210_info_pag.idedmdev ||
                     '</ideDmDev>' || /* dalves - s1210 - 18/11/2021
                     chr(10) || '<indPgtoTt>' ||
                     c_1210_info_pag.indpgtott || '</indPgtoTt>' ||*/ chr(10) ||
                     '<vrLiq>' || c_1210_info_pag.vrliq || '</vrLiq>' ||
                     chr(10);


          v_xml := v_xml || '</infoPgto>' || chr(10);

        --/c_1210_info_pag
        END LOOP;
        
          v_xml := v_xml  || '</ideBenef>' || chr(10) || '</evtPgtos>' || chr(10) || '</eSocial>';

        --Atualiza Evento
        UPDATE esocial.tsoc_1210_pag_rendimentos pr
           SET pr.xml_envio       = v_xml,
               pr.ctr_flg_status  = 'AA',
               pr.dat_ult_atu     = SYSDATE,
               pr.nom_usu_ult_atu = USER,
               pr.nom_pro_ult_atu = 'SP_XML_1210'
         WHERE pr.id_pk = c_1210_pag_rend.id_pk
           AND pr.flg_vigencia = 'A'
           AND pr.ctr_flg_status = 'AX';

        COMMIT;

        v_qtd_reg := v_qtd_reg + 1;

        --Atualiza quantidade
        sp_seta_processo(p_id_ctr_processo,
                         'ATUALIZA_QUANTIDADE',
                         v_qtd_reg);

      EXCEPTION
        WHEN OTHERS THEN
          --Atualiza Evento Erro
          UPDATE esocial.tsoc_1210_pag_rendimentos pr
             SET pr.xml_envio       = v_xml,
                 pr.ctr_flg_status  = 'EX',
                 pr.dat_ult_atu     = SYSDATE,
                 pr.nom_usu_ult_atu = USER,
                 pr.nom_pro_ult_atu = 'SP_XML_1210'
           WHERE pr.id_pk = c_1210_pag_rend.id_pk
             AND pr.flg_vigencia = 'A'
             AND pr.ctr_flg_status = 'AX';
          COMMIT;

          gb_rec_erro.cod_ins           := c_1210_pag_rend.cod_ins;
          gb_rec_erro.id_cad            := c_1210_pag_rend.id_pk;
          gb_rec_erro.nom_processo      := 'SP_XML_1210';
          gb_rec_erro.id_evento         := c_1210_pag_rend.id_evento;
          gb_rec_erro.desc_erro         := 'ERRO AO GERAR XML PARA O EVENTO';
          gb_rec_erro.desc_erro_bd      := SQLERRM;
          gb_rec_erro.des_identificador := NULL;
          gb_rec_erro.flg_tipo_erro     := 'X';
          gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
          sp_gera_erro_processo;
          sp_seta_processo(p_id_ctr_processo, 'ERRO_PROCESSAMENTO', NULL);

      END;

    --/c_1210_pag_rend
    END LOOP;

    sp_seta_processo(p_id_ctr_processo, 'FIM_PROCESSAMENTO', v_qtd_reg);

  EXCEPTION
    WHEN ex_param_proc THEN
      gb_rec_erro.cod_ins           := NULL;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_1210';
      gb_rec_erro.id_evento         := NULL;
      gb_rec_erro.desc_erro         := 'ERRO NA PARAMETRIZAC?O DO PROCESSO';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo, 'ERRO_PROCESSAMENTO', NULL);

    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := NULL;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_1210_RENDIMENTOS';
      gb_rec_erro.id_evento         := NULL;
      gb_rec_erro.desc_erro         := 'ERRO DURANTE A EXECUC?O DO PROCESSO';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo, 'ERRO_PROCESSAMENTO', NULL);
  END sp_xml_1210;

  PROCEDURE sp_xml_1210_t(p_id_ctr_processo IN esocial.tsoc_ctr_processo.id_ctr_processo%TYPE) IS
    v_xml CLOB;
    ex_param_proc EXCEPTION;
    v_id_pk          esocial.tsoc_1210_pag_rendimentos.id_pk%TYPE;
    v_id_pgto        esocial.tsoc_cpl_1210_info_pgto.id_pgto%TYPE;
    --v_id_pgto_ben_pr esocial.tsoc_cpl_1210_det_pgto_ben_pr.id_pgto_ben_pr%TYPE;
    v_qtd_reg        NUMBER := 0;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;
  BEGIN

    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,''';
    
    --Valida Processo
    BEGIN
      sp_carrega_ids(p_id_ctr_processo);
    EXCEPTION
      WHEN OTHERS THEN
        RAISE ex_param_proc;
    END;

    sp_seta_processo(p_id_ctr_processo, 'INICIO_PROCESSAMENTO', NULL);

    SELECT ctr.faixa_ini, ctr.faixa_fim
      INTO v_faixa_ini, v_faixa_fim
      FROM tsoc_ctr_processo ctr
     WHERE id_ctr_processo = p_id_ctr_processo;

    FOR c_1210_pag_rend IN (SELECT t.*, e.xmlns
                              FROM esocial.tsoc_1210_pag_rendimentos_t t,
                                   esocial.tsoc_par_evento           e
                             WHERE t.ctr_flg_status = 'AX'
                               AND (t.id_pk BETWEEN v_faixa_ini AND
                                   v_faixa_fim OR (v_faixa_ini IS NULL AND
                                   v_faixa_fim IS NULL))
                               AND t.flg_vigencia = 'A'
                                  --1210
                               AND e.id_evento = 9) LOOP

      BEGIN
        v_xml :=  --eSocial------------------------------------------------------------------------------------------
         '<eSocial xmlns="' || c_1210_pag_rend.xmlns || '">' ||
                 chr(10) ||
                --evtPgtos-----------------------------------------------------------------------------------------
                 '<evtPgtos Id ="' || c_1210_pag_rend.id || '">' || chr(10) ||
                --ideEvento----------------------------------------------------------------------------------------
                 '<ideEvento>' || chr(10) || '<indRetif>' ||
                 c_1210_pag_rend.indretif || '</indRetif>' || chr(10);
        IF c_1210_pag_rend.nrrecibo IS NOT NULL THEN
          v_xml := v_xml || '<nrRecibo>' || c_1210_pag_rend.nrrecibo ||
                   '</nrRecibo>' || chr(10);
        END IF;
        v_xml := v_xml || /* dalves - s1210 - 18/11/2021
        '<indApuracao>' || c_1210_pag_rend.indapuracao ||
                 '</indApuracao>' || chr(10) ||*/ '<perApur>' ||
                 c_1210_pag_rend.perapur || '</perApur>' || chr(10) ||
                 '<tpAmb>' || c_1210_pag_rend.tpamb || '</tpAmb>' ||
                 chr(10) || '<procEmi>' || c_1210_pag_rend.procemi ||
                 '</procEmi>' || chr(10) || '<verProc>' ||
                 c_1210_pag_rend.verproc || '</verProc>' || chr(10) ||
                 '</ideEvento>' || chr(10);
        v_xml := v_xml ||
                --ideEmpregador------------------------------------------------------------------------------------
                 '<ideEmpregador>' || chr(10) || '<tpInsc>' ||
                 c_1210_pag_rend.tpinsc || '</tpInsc>' || chr(10) ||
                 '<nrInsc>' || c_1210_pag_rend.nrinsc || '</nrInsc>' ||
                 chr(10) || '</ideEmpregador>' || chr(10) ||
                --ideBenef-----------------------------------------------------------------------------------------
                 '<ideBenef>' || chr(10) || '<cpfBenef>' ||
                 c_1210_pag_rend.cpfbenef || '</cpfBenef>' || chr(10);
        --deps---------------------------------------------------------------------------------------------
       /* dalves - s1210 - 18/11/2021
        IF c_1210_pag_rend.vrdeddep IS NOT NULL THEN
          v_xml := v_xml || '<deps>' || chr(10) || '<vrDedDep>' ||
                   c_1210_pag_rend.vrdeddep || '</vrDedDep>' || chr(10) ||
                   '</deps>' || chr(10);
        END IF;*/

        v_id_pk := c_1210_pag_rend.id_pk;

        --Cursor InfoPgto
        FOR c_1210_info_pag IN (SELECT i.id_pgto,
                                       i.id_pk,
                                       to_char(i.dtpgto,'RRRR-MM-DD') as dtpgto,
                                       i.tppgto,
                                       i.indresbr,
                                       pr.id_pgto_ben_pr,
                                       pr.perref,
                                       pr.idedmdev,
                                       pr.indpgtott,
                                       to_char(TRUNC(pr.vrliq, 2),'FM999999999999.90') as vrliq
                                  FROM esocial.tsoc_cpl_1210_info_pgto_t1       i,
                                       esocial.tsoc_cpl_1210_det_pgto_ben_pr_t pr
                                 WHERE i.id_pk = v_id_pk
                                   and i.id_pgto = pr.id_pgto) LOOP

          v_id_pgto := c_1210_info_pag.id_pgto;

          v_xml := v_xml ||
                  --infoPgto------------------------------------------------------------------------------------------
                   '<infoPgto>' || chr(10) || '<dtPgto>' ||
                   c_1210_info_pag.dtpgto || '</dtPgto>' || chr(10) ||
                   '<tpPgto>' || c_1210_info_pag.tppgto || '</tpPgto>' ||
                   chr(10); /* dalves - s1210 18/11/2021
                   '<indResBr>' || c_1210_info_pag.indresbr ||
                   '</indResBr>' || chr(10) ||*/ 


            v_xml := v_xml ||
                    --detPgtoBenPr------------------------------------------------------------------------------------------
                     '<perRef>' ||
                     c_1210_info_pag.perref || '</perRef>' || chr(10) ||
                     '<ideDmDev>' || c_1210_info_pag.idedmdev ||
                     '</ideDmDev>' || /* dalves - s1210 - 18/11/2021
                     chr(10) || '<indPgtoTt>' ||
                     c_1210_info_pag.indpgtott || '</indPgtoTt>' ||*/ chr(10) ||
                     '<vrLiq>' || c_1210_info_pag.vrliq || '</vrLiq>' ||
                     chr(10);


          v_xml := v_xml || '</infoPgto>' || chr(10);

        --/c_1210_info_pag
        END LOOP;
        
          v_xml := v_xml  || '</ideBenef>' || chr(10) || '</evtPgtos>' || chr(10) || '</eSocial>';

        --Atualiza Evento
        UPDATE esocial.tsoc_1210_pag_rendimentos_t pr
           SET pr.xml_envio       = v_xml,
               pr.ctr_flg_status  = 'AA',
               pr.dat_ult_atu     = SYSDATE,
               pr.nom_usu_ult_atu = USER,
               pr.nom_pro_ult_atu = 'SP_XML_1210'
         WHERE pr.id_pk = c_1210_pag_rend.id_pk
           AND pr.flg_vigencia = 'A'
           AND pr.ctr_flg_status = 'AX';

        COMMIT;

        v_qtd_reg := v_qtd_reg + 1;

        --Atualiza quantidade
        sp_seta_processo(p_id_ctr_processo,
                         'ATUALIZA_QUANTIDADE',
                         v_qtd_reg);

      EXCEPTION
        WHEN OTHERS THEN
          --Atualiza Evento Erro
          UPDATE esocial.tsoc_1210_pag_rendimentos_t pr
             SET pr.xml_envio       = v_xml,
                 pr.ctr_flg_status  = 'EX',
                 pr.dat_ult_atu     = SYSDATE,
                 pr.nom_usu_ult_atu = USER,
                 pr.nom_pro_ult_atu = 'SP_XML_1210'
           WHERE pr.id_pk = c_1210_pag_rend.id_pk
             AND pr.flg_vigencia = 'A'
             AND pr.ctr_flg_status = 'AX';
          COMMIT;

          gb_rec_erro.cod_ins           := c_1210_pag_rend.cod_ins;
          gb_rec_erro.id_cad            := c_1210_pag_rend.id_pk;
          gb_rec_erro.nom_processo      := 'SP_XML_1210';
          gb_rec_erro.id_evento         := c_1210_pag_rend.id_evento;
          gb_rec_erro.desc_erro         := 'ERRO AO GERAR XML PARA O EVENTO';
          gb_rec_erro.desc_erro_bd      := SQLERRM;
          gb_rec_erro.des_identificador := NULL;
          gb_rec_erro.flg_tipo_erro     := 'X';
          gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
          sp_gera_erro_processo;
          sp_seta_processo(p_id_ctr_processo, 'ERRO_PROCESSAMENTO', NULL);

      END;

    --/c_1210_pag_rend
    END LOOP;

    sp_seta_processo(p_id_ctr_processo, 'FIM_PROCESSAMENTO', v_qtd_reg);

  EXCEPTION
    WHEN ex_param_proc THEN
      gb_rec_erro.cod_ins           := NULL;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_1210';
      gb_rec_erro.id_evento         := NULL;
      gb_rec_erro.desc_erro         := 'ERRO NA PARAMETRIZAC?O DO PROCESSO';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo, 'ERRO_PROCESSAMENTO', NULL);

    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := NULL;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_1210_RENDIMENTOS';
      gb_rec_erro.id_evento         := NULL;
      gb_rec_erro.desc_erro         := 'ERRO DURANTE A EXECUC?O DO PROCESSO';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo, 'ERRO_PROCESSAMENTO', NULL);
  END sp_xml_1210_t;

  PROCEDURE sp_xml_1299(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --    vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_1299(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))

         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_1299';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_1299';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_1299;

    FUNCTION fc_tag_1299(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))

           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_1299';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_1299';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_1299;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_1299';
        gb_rec_erro.id_evento         := 1;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk
                FROM tsoc_1299_fechamento_ep
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX'
                 AND flg_vigencia = 'A') LOOP

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = 1

         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtFechaEvPer'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 1299;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtFechaEvPer'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql = 1))

                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_1299(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtFechaEvPer',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_1299(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtFechaEvPer',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_1299(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtFechaEvPer',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))

                 AND tev.nom_evento = 'evtFechaEvPer'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtFechaEvPer'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql = 1))

                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_1299';
                gb_rec_erro.id_evento         := 1;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 1299';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_1299(afechatag(i),
                                v_cod_ins,
                                'evtFechaEvPer',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);

            /*IF x != 1 THEN
              v_valores := PAC_ESOCIAL_EVENTOS_NP.fc_formata_string(v_valores);
            END IF;*/

            cxml := fc_set_valor_1299('evtFechaEvPer',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          --dalves 09/02/2022
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);


          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --              dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_1299';
            gb_rec_erro.id_evento         := 1;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1299';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_1299';
      gb_rec_erro.id_evento         := 1;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1299';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_1299;

  PROCEDURE sp_xml_1298(p_id_ctr_processo IN NUMBER) IS

    cur_tag          SYS_REFCURSOR;
    cur_count        NUMBER;
    cur_desc         dbms_sql.desc_tab;
    n_cursor_control NUMBER;
    vtab_update      VARCHAR2(30);
    v_qtd_registros  NUMBER := 0;
    v_cod_ins        NUMBER;
    v_faixa_ini      NUMBER;
    v_faixa_fim      NUMBER;

    TYPE t_array IS TABLE OF tsoc_par_estruturas_xml.nom_registro%TYPE INDEX BY PLS_INTEGER;
    afechatag t_array;

    cxml          CLOB;
    vdsc_sql      tsoc_par_sql_xml.dsc_sql%TYPE;
    nfechatag     NUMBER;
    vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;
    --    vdata_ini     DATE;
    --    vdata_fim     DATE;

    vnum_versao_xml      tsoc_par_eventos_xml.num_versao_xml%TYPE;
    vdsc_encoding_xml    tsoc_par_eventos_xml.dsc_encoding_xml%TYPE;
    vnum_cnpj_empregador tsoc_par_eventos_xml.num_cnpj_empregador%TYPE;

    v_valores VARCHAR2(100);

    FUNCTION fc_set_valor_1298(p_nom_evento     IN VARCHAR2,
                               p_cod_ins        IN NUMBER,
                               p_xml            IN CLOB,
                               p_valor          VARCHAR2,
                               p_num_seq_coluna NUMBER) RETURN CLOB IS

      vnom_registro        tsoc_par_estruturas_xml.nom_registro%TYPE;
      nqtd_maxima_registro tsoc_par_estruturas_xml.qtd_maxima_registro%TYPE;
      vtip_elemento        tsoc_par_estruturas_xml.tip_elemento%TYPE;

      vxml   CLOB;
      vvalor VARCHAR2(100);

      --      raise_tam_invalido EXCEPTION;
    BEGIN

      vvalor := p_valor;

      SELECT tet.nom_registro, tet.qtd_maxima_registro, tet.tip_elemento
        INTO vnom_registro, nqtd_maxima_registro, vtip_elemento
        FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = p_cod_ins
         AND tev.nom_evento = p_nom_evento
         AND tev.dat_fim_vig IS NULL
         AND (tet.flg_obrigatorio = 'S' OR
             (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))

         AND tet.num_seq_coluna = p_num_seq_coluna;

      -- seto o valor no xml, dentro da tag passada como parametro

      IF vtip_elemento = 'A' THEN

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || '"' || vvalor || '"' ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      ELSE

        vxml := substr(p_xml,
                       1,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro)) || vvalor ||
                substr(p_xml,
                       (instr(p_xml, vnom_registro, 1)) +
                       length(vnom_registro) + 1);

      END IF;
      RETURN vxml;

    EXCEPTION

      WHEN no_data_found THEN
        RETURN p_xml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_SET_VALOR_1298';
        gb_rec_erro.id_evento         := 12;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_SET_VALOR_1298';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_set_valor_1298;

    FUNCTION fc_tag_1298(p_nom_registro IN VARCHAR2,
                         p_cod_ins      IN NUMBER,
                         p_nom_evento   IN VARCHAR2,
                         p_abre_fecha   IN VARCHAR2) RETURN VARCHAR2 IS

      vxml          VARCHAR2(100);
      vnom_registro tsoc_par_estruturas_xml.nom_registro%TYPE;

    BEGIN

      -- identifico se o parametro e para abertura de tag

      IF p_abre_fecha = 'A' THEN

        -- verifico se ha algum atributo a ser atribuido a tag de abertura e o seu respectivo valor

        SELECT tet.nom_registro
          INTO vnom_registro
          FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
         WHERE tev.cod_ins = tet.cod_ins
           AND tev.cod_evento = tet.cod_evento
           AND tev.num_versao_evento = tet.num_versao_evento
           AND tev.cod_ins = p_cod_ins
           AND tev.nom_evento = p_nom_evento
           AND tet.tip_elemento = 'A'
           AND tev.dat_fim_vig IS NULL
           AND (tet.flg_obrigatorio = 'S' OR
               (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))

           AND tet.nom_registro_pai = p_nom_registro;

        vxml := '<' || p_nom_registro || ' ' || vnom_registro || '=' || '>';

      ELSIF p_abre_fecha = 'F' THEN

        vxml := '</' || p_nom_registro || '>';

      END IF;

      RETURN vxml;

    EXCEPTION
      WHEN no_data_found THEN

        -- caso n?o exista atributo definido para a tag, apenas a abro
        vxml := '<' || p_nom_registro || '>';
        RETURN vxml;

      WHEN OTHERS THEN

        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'FC_TAG_1298';
        gb_rec_erro.id_evento         := 12;
        gb_rec_erro.desc_erro         := 'ERRO NA FUNC?O FC_TAG_1298';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;

    END fc_tag_1298;

  BEGIN

    gb_rec_erro.id_ctr_processo := p_id_ctr_processo;

    --    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_IPESP.TSOC_ANALISE_TEMPO_GERACAO';

    -- Retorno query para montar cursor do detalhe do XML e informac?es fixas do header

    BEGIN

      SELECT ctr.cod_ins, ctr.faixa_ini, ctr.faixa_fim
        INTO v_cod_ins, v_faixa_ini, v_faixa_fim
        FROM tsoc_ctr_processo ctr
       WHERE id_ctr_processo = p_id_ctr_processo
         AND flg_status = 'A';

    EXCEPTION
      WHEN OTHERS THEN
        gb_rec_erro.cod_ins           := v_cod_ins;
        gb_rec_erro.id_cad            := NULL;
        gb_rec_erro.nom_processo      := 'SP_XML_1298';
        gb_rec_erro.id_evento         := 12;
        gb_rec_erro.desc_erro         := 'NAO FOI LOCALIZADO PERIODO PARAMETRIZADO';
        gb_rec_erro.desc_erro_bd      := SQLERRM;
        gb_rec_erro.des_identificador := NULL;
        gb_rec_erro.flg_tipo_erro     := 'X';
        gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
        sp_gera_erro_processo;
        sp_seta_processo(p_id_ctr_processo,
                         'ERRO_PROCESSAMENTO',
                         v_qtd_registros);
    END;

    sp_seta_processo(p_id_ctr_processo,
                     'INICIO_PROCESSAMENTO',
                     v_qtd_registros);

    FOR x IN (SELECT id_pk
                FROM tsoc_1298_reabertura_ep
               WHERE (id_pk BETWEEN v_faixa_ini AND v_faixa_fim OR
                     v_faixa_ini IS NULL)
                 AND ctr_flg_status = 'AX'
                 AND flg_vigencia = 'A') LOOP

      afechatag.delete();

      SELECT tsx.dsc_sql,
             tev.num_versao_xml,
             tev.dsc_encoding_xml,
             tev.num_cnpj_empregador
        INTO vdsc_sql,
             vnum_versao_xml,
             vdsc_encoding_xml,
             vnum_cnpj_empregador
        FROM tsoc_par_eventos_xml    tev,
             tsoc_par_estruturas_xml tet,
             tsoc_par_sql_xml        tsx
       WHERE tev.cod_ins = tet.cod_ins
         AND tev.cod_evento = tet.cod_evento
         AND tev.num_versao_evento = tet.num_versao_evento
         AND tev.cod_ins = tsx.cod_ins
         AND tev.cod_evento = tsx.cod_evento
         AND tev.num_versao_evento = tsx.num_versao_evento
         AND tsx.num_seq_sql = 1

         AND tev.cod_ins = v_cod_ins
         AND tev.nom_evento = 'evtReabreEvPer'
         AND tev.dat_fim_vig IS NULL
         AND tet.num_seq = 1
         AND tet.flg_sql = 'S'
         AND tet.flg_obrigatorio = 'S';

      SELECT nom_tabela
        INTO vtab_update
        FROM tsoc_par_evento
       WHERE cod_evento = 1298;
      -- vtab_update := substr(vdsc_sql, instr(vdsc_sql, 'FROM') + 5);

      vdsc_sql := vdsc_sql || ' AND id_pk = ' || x.id_pk;

      OPEN cur_tag FOR vdsc_sql;

      -- atribuo um id referencia ao cursor e defino as colunas da query no cursor

      n_cursor_control := dbms_sql.to_cursor_number(cur_tag);
      dbms_sql.describe_columns(n_cursor_control, cur_count, cur_desc);

      FOR x IN 1 .. cur_count LOOP

        -- percorro o cursor e defino os valores para cada coluna

        dbms_sql.define_column(n_cursor_control, x, v_valores, 4000);

      END LOOP;

      WHILE dbms_sql.fetch_rows(n_cursor_control) > 0 LOOP
        BEGIN
          cxml := NULL;
          -- variavel para controlar array de fechamento das tags
          nfechatag := 1;

          /*          cxml := '<?xml version="' || vnum_versao_xml || '" encoding="' ||
          vdsc_encoding_xml || '"?>' || chr(13);*/

          -- identifico todas as tags parametrizadas na tabela e que deverao ser inseridas no arquivo

          FOR c_tag IN (SELECT tet.nom_registro,
                               tet.nom_registro_pai,
                               tet.tip_elemento,
                               tet.flg_sql,
                               tet.num_seq_sql,
                               tet.num_seq_coluna
                          FROM tsoc_par_eventos_xml    tev,
                               tsoc_par_estruturas_xml tet
                         WHERE tev.cod_ins = tet.cod_ins
                           AND tev.cod_evento = tet.cod_evento
                           AND tev.num_versao_evento = tet.num_versao_evento
                           AND tev.cod_ins = v_cod_ins
                           AND tev.nom_evento = 'evtReabreEvPer'
                           AND tet.tip_elemento IN ('G', 'CG', 'E')
                           AND tev.dat_fim_vig IS NULL
                           AND (tet.flg_obrigatorio = 'S' OR
                               (tet.flg_obrigatorio = 'N' AND
                               tet.num_seq_sql = 1))

                         ORDER BY num_seq ASC) LOOP

            -- identifico se e uma tag de grupo (tags que n?o possuem valores associados, apenas atributos)
            IF c_tag.tip_elemento IN ('G', 'CG') THEN

              -- adiciono no array auxiliar para fechamento das tags

              afechatag(nfechatag) := c_tag.nom_registro;

              -- chamo a func de montar tag, passando parametro de abertura de tag

              cxml := cxml || fc_tag_1298(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtReabreEvPer',
                                          'A') || chr(13);

              nfechatag := nfechatag + 1;
            ELSE
              -- caso seja uma tag de elemento (tags que possuem valor associado)

              cxml := cxml || fc_tag_1298(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtReabreEvPer',
                                          'A');

              -- chamo func de montar tag com parametro de fechamento de tag
              cxml := cxml || fc_tag_1298(c_tag.nom_registro,
                                          v_cod_ins,
                                          'evtReabreEvPer',
                                          'F') || chr(13);

            END IF;

          END LOOP;

          -- cursor para fechamento das tags de grupo

          FOR i IN REVERSE 1 .. afechatag.count LOOP

            -- identifico a hierarquia das tags a partir do registro pai e, consequentemente, o ponto
            -- onde devemos fechar a tag
            BEGIN

              SELECT tet.nom_registro
                INTO vnom_registro
                FROM tsoc_par_eventos_xml tev, tsoc_par_estruturas_xml tet
               WHERE tev.cod_ins = tet.cod_ins
                 AND tev.cod_evento = tet.cod_evento
                 AND tev.num_versao_evento = tet.num_versao_evento
                 AND tev.cod_ins = v_cod_ins
                 AND tet.tip_elemento IN ('G', 'CG', 'E')
                 AND tev.dat_fim_vig IS NULL
                 AND (tet.flg_obrigatorio = 'S' OR
                     (tet.flg_obrigatorio = 'N' AND tet.num_seq_sql = 1))

                 AND tev.nom_evento = 'evtReabreEvPer'
                 AND tet.nom_registro_pai = afechatag(i)
                 AND num_seq =
                     (SELECT MAX(num_seq)
                        FROM tsoc_par_eventos_xml    tev,
                             tsoc_par_estruturas_xml tet
                       WHERE tev.cod_ins = tet.cod_ins
                         AND tev.cod_evento = tet.cod_evento
                         AND tev.num_versao_evento = tet.num_versao_evento
                         AND tev.cod_ins = v_cod_ins
                         AND tev.nom_evento = 'evtReabreEvPer'
                         AND tet.tip_elemento IN ('G', 'CG', 'E')
                         AND tev.dat_fim_vig IS NULL
                         AND (tet.flg_obrigatorio = 'S' OR
                             (tet.flg_obrigatorio = 'N' AND
                             tet.num_seq_sql = 1))

                         AND tet.nom_registro_pai = afechatag(i));

            EXCEPTION
              WHEN OTHERS THEN
                v_qtd_registros               := v_qtd_registros + 1;
                gb_rec_erro.cod_ins           := v_cod_ins;
                gb_rec_erro.id_cad            := x.id_pk;
                gb_rec_erro.nom_processo      := 'SP_XML_1298';
                gb_rec_erro.id_evento         := 12;
                gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE FECHAR TAGS DO XML 1298';
                gb_rec_erro.desc_erro_bd      := afechatag(i) || ' - ' ||
                                                 g_tp_beneficio || ' - ' ||
                                                 SQLERRM;
                gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
                gb_rec_erro.des_identificador := NULL;
                gb_rec_erro.flg_tipo_erro     := 'X';
                sp_gera_erro_processo;
            END;
            -- identifico o ponto onde devera ser fechado a tag e chamo a func passando parametro de fechamento
            cxml := substr(cxml,
                           1,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1) ||
                    fc_tag_1298(afechatag(i),
                                v_cod_ins,
                                'evtReabreEvPer',
                                'F') ||
                    substr(cxml,
                           (instr(cxml, vnom_registro, -1)) +
                           length(vnom_registro) + 1);

          END LOOP;

          FOR x IN 1 .. cur_count LOOP
            -- seta no xml os valores retornados pelo cursor parametrizado

            dbms_sql.column_value(n_cursor_control, x, v_valores);

            /*IF x NOT IN (1,4) THEN
              v_valores := PAC_ESOCIAL_EVENTOS_NP.fc_formata_string(v_valores);
            END IF;*/

            cxml := fc_set_valor_1298('evtReabreEvPer',
                                      v_cod_ins,
                                      cxml,
                                      v_valores,
                                      to_number(cur_desc(x).col_name));

          END LOOP;
          
          --limpa tag
          --dalves 09/02/2022
          cxml := esocial.limpa_tag(esocial.limpa_tag(cxml,1),2);


          EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                            ' SET XML_ENVIO = ''' || cxml ||
                            ''', CTR_FLG_STATUS = ''AA'' WHERE ID_PK = ' ||
                            x.id_pk;
          COMMIT;

          --              dbms_output.put_line(cxml);

          v_qtd_registros := v_qtd_registros + 1;

          sp_seta_processo(p_id_ctr_processo,
                           'ATUALIZA_QUANTIDADE',
                           v_qtd_registros);
        EXCEPTION
          WHEN OTHERS THEN
            v_qtd_registros               := v_qtd_registros + 1;
            gb_rec_erro.cod_ins           := v_cod_ins;
            gb_rec_erro.id_cad            := x.id_pk;
            gb_rec_erro.nom_processo      := 'SP_XML_1298';
            gb_rec_erro.id_evento         := 12;
            gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1298';
            gb_rec_erro.desc_erro_bd      := SQLERRM;
            gb_rec_erro.des_identificador := NULL;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            sp_gera_erro_processo;

            EXECUTE IMMEDIATE 'UPDATE ' || vtab_update ||
                              ' SET CTR_FLG_STATUS = ''EX'' WHERE ID_PK = ' ||
                              x.id_pk;
            COMMIT;

        END;

      END LOOP;

      dbms_sql.close_cursor(n_cursor_control);

      v_faixa_ini := v_faixa_ini + 1;

    END LOOP;

    sp_seta_processo(p_id_ctr_processo,
                     'FIM_PROCESSAMENTO',
                     v_qtd_registros);

  EXCEPTION
    WHEN OTHERS THEN
      gb_rec_erro.cod_ins           := 1;
      gb_rec_erro.id_cad            := NULL;
      gb_rec_erro.nom_processo      := 'SP_XML_1298';
      gb_rec_erro.id_evento         := 12;
      gb_rec_erro.desc_erro         := 'ERRO NO PROCESSO DE GERAC?O DO XML 1298';
      gb_rec_erro.desc_erro_bd      := SQLERRM;
      gb_rec_erro.des_identificador := NULL;
      gb_rec_erro.flg_tipo_erro     := 'X';
      gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
      sp_gera_erro_processo;
      sp_seta_processo(p_id_ctr_processo,
                       'ERRO_PROCESSAMENTO',
                       v_qtd_registros);

  END sp_xml_1298;


  PROCEDURE sp_arqs_qualificacao_cadastral AS

    vfile1 VARCHAR2(1000); --Aposentadoria Civil
    vtype1 utl_file.file_type;

    vfile2 VARCHAR2(1000); --Aposentadoria Militar
    vtype2 utl_file.file_type;

    vfile3 VARCHAR2(1000); --Pens?o Civil
    vtype3 utl_file.file_type;

    vfile4 VARCHAR2(1000); --Pens?o Militar
    vtype4 utl_file.file_type;

    vdir VARCHAR2(25) := 'ARQS_REL_GERAIS';

    CURSOR c_aposen_civil IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;

    CURSOR c_aposen_militar IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio <> 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;

    CURSOR c_pensao_civil IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade <> 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;

    CURSOR c_pensao_militar IS
      WITH got_tenth AS
       (SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               ntile(10) over(ORDER BY pf.cod_ide_cli) AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
        UNION
        SELECT regexp_replace(lpad(pf.num_cpf, 11, '0'), '[^0-9]', '') cpf,
               regexp_replace(nvl(se.num_pis, '11111111116'), '[^0-9]', '') nis,
               TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                  '[^[:alpha:][ ]]*',
                                                  ''),
                                   '[[:space:]]+',
                                   chr(32))) nome,
               to_char(pf.dat_nasc, 'DDMMRRRR') "DN",
               10 AS tenth
          FROM user_ipesp.tb_servidor            se,
               user_ipesp.tb_pessoa_fisica       pf,
               user_ipesp.tb_concessao_beneficio cb,
               user_ipesp.tb_beneficiario        ben
         WHERE cb.cod_tipo_beneficio = 'M'
           AND cb.cod_entidade = 5
           AND cb.cod_ins = se.cod_ins
           AND cb.cod_ide_cli_serv = se.cod_ide_cli
           AND cb.cod_ins = ben.cod_ins
           AND cb.cod_beneficio = ben.cod_beneficio
           AND ben.cod_ins = pf.cod_ins
           AND ben.cod_ide_cli_ben = pf.cod_ide_cli
           AND ben.flg_status IN ('A', 'S')
           AND length(TRIM(regexp_replace(regexp_replace(pf.nom_pessoa_fisica,
                                                         '[^[:alpha:][ ]]*',
                                                         ''),
                                          '[[:space:]]+',
                                          chr(32)))) > 60)
      SELECT cpf || ';' || nis || ';' || nome || ';' || "DN" AS vs_dados
        FROM got_tenth
       WHERE rownum < 1001
       ORDER BY nome;

  BEGIN

    --1. Aposentadoria Civil
    vfile1 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.APOSENTADORIA_CIVIL.TXT';

    vtype1 := utl_file.fopen(vdir, vfile1, 'W', 32767);

    FOR reg IN c_aposen_civil LOOP

      --Gravando a linha
      utl_file.put_line(vtype1, reg.vs_dados);

    END LOOP;

    utl_file.fclose(vtype1);

    --2. Aposentadoria Militar
    vfile2 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.APOSENTADORIA_MILITAR.TXT';

    vtype2 := utl_file.fopen(vdir, vfile2, 'W', 32767);

    FOR reg IN c_aposen_militar LOOP

      --Gravando a linha
      utl_file.put_line(vtype2, reg.vs_dados);

    END LOOP;

    utl_file.fclose(vtype2);

    --3. Pens?o Civil
    vfile3 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.PENSAO_CIVIL.TXT';

    vtype3 := utl_file.fopen(vdir, vfile3, 'W', 32767);

    FOR reg IN c_pensao_civil LOOP

      --Gravando a linha
      utl_file.put_line(vtype3, reg.vs_dados);

    END LOOP;

    utl_file.fclose(vtype3);

    --4. Pens?o Militar
    vfile4 := 'D.CNS.CPF.001.' || to_char(SYSDATE, 'RRRRMMDDHH24MISS') ||
              '.PENSAO_MILITAR.TXT';

    vtype4 := utl_file.fopen(vdir, vfile4, 'W', 32767);

    FOR reg IN c_pensao_militar LOOP

      --Gravando a linha
      utl_file.put_line(vtype4, reg.vs_dados);

    END LOOP;

    utl_file.fclose(vtype4);

  EXCEPTION
    WHEN OTHERS THEN
      utl_file.fclose(vtype1);
      utl_file.fclose(vtype2);
      utl_file.fclose(vtype3);
      utl_file.fclose(vtype4);
  END sp_arqs_qualificacao_cadastral;
  
    PROCEDURE SP_ATUALIZA_EVENTO(P_ID_PK        IN NUMBER,
                                 P_TABELA       IN VARCHAR2,
                                 P_XML_ASSINADO IN CLOB,
                                 P_COD_INS      IN NUMBER,
                                 P_TIP_ATU      IN VARCHAR2
                                 ) IS
  
  BEGIN

    IF P_TABELA = 'TSOC_2400_DEPENDENTE' THEN
      UPDATE ESOCIAL.TSOC_2400_DEPENDENTE
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;
         
    ELSIF P_TABELA = 'TSOC_2410_BENEFICIO_INI' THEN
      UPDATE ESOCIAL.TSOC_2410_BENEFICIO_INI
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;
      
    ELSIF P_TABELA = 'TSOC_2400_BENEFICIARIO_INI' THEN
      UPDATE ESOCIAL.TSOC_2400_BENEFICIARIO_INI
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;
           
      ELSIF P_TABELA = 'TSOC_2405_BENEFICIARIO_ALT' THEN
      UPDATE ESOCIAL.TSOC_2405_BENEFICIARIO_ALT
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;     
           
         ELSIF P_TABELA = 'TSOC_2416_BENEFICIO_ALT' THEN
      UPDATE ESOCIAL.TSOC_2416_BENEFICIO_ALT
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;     
           
       ELSIF P_TABELA = 'TSOC_2420_BENEFICIO_TERMINO' THEN
      UPDATE ESOCIAL.TSOC_2420_BENEFICIO_TERMINO
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;     

       ELSIF P_TABELA = 'TSOC_2418_BENEFICIO_REATIVACAO' THEN
      UPDATE ESOCIAL.TSOC_2418_BENEFICIO_REATIVACAO
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;
                      
         ELSIF P_TABELA = 'TSOC_1000_EMPREGADOR' THEN
      UPDATE ESOCIAL.TSOC_1000_EMPREGADOR
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;   

       ELSIF P_TABELA = 'TSOC_1010_RUBRICA' THEN
      UPDATE ESOCIAL.TSOC_1010_RUBRICA
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK
         AND COD_INS = P_COD_INS;   
           
           
         ELSIF P_TABELA = 'TSOC_1207_BENEFICIO' THEN
      UPDATE ESOCIAL.TSOC_1207_BENEFICIO
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK;
         
         ELSIF P_TABELA = 'TSOC_1210_PAG_RENDIMENTOS' THEN
      UPDATE ESOCIAL.TSOC_1210_PAG_RENDIMENTOS
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK;         
              
         ELSIF P_TABELA = 'TSOC_1298_REABERTURA_EP' THEN
      UPDATE ESOCIAL.TSOC_1298_REABERTURA_EP
         SET XML_ENVIO       = P_XML_ASSINADO,
             CTR_FLG_STATUS  = P_TIP_ATU,
             DAT_ULT_ATU     = SYSDATE,
             NOM_USU_ULT_ATU = 'ESOCIAL',
             NOM_PRO_ULT_ATU = 'SP_ATUALIZA_EVENTO'
       WHERE ID_PK = P_ID_PK;  
       
    END IF;          
  
    COMMIT;
  
  END SP_ATUALIZA_EVENTO;

 

END pac_esocial_xml_102;

/
