CREATE OR REPLACE package         PAC_ESOCIAL_EVENTOS_PE_102 is

    /* -----------------------------------------------------------------------------------------------
    Descricao: eSocial - Package de processamento de eventos periódicos

    Versão 1.02

    Histórico de versões

    - 1.02 - 12/06/2019 - Francisco Cavalcante -
           - Modificação no procedimento de processamento CAD_FOLHA/CAD_DET_FOLHA para melhoria de
           performance
           - Adequação para permitir alteração de dados individuais dos processos 1207, 1210 e 1299

    - 1.01 - 27/05/2019 - Francisco Cavalcante - T55645
           - Ajuste para evitar duplicidade de ID de evento em execuções concorrentes

    - 1.00 - 19/12/2018 10:39:33 - LUCAS PEREIRA
           - Criação.
    ----------------------------------------------------------------------------------------------- */

    PROCEDURE SP_CAD_FOLHA_1207(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);
    --TICKET 84697 - DALVES - 18/01/2023
    --GERA RETIFICAÇÃO DE FOLHA E EVENTO 1207 DE BENEFÍCIOS - ENTES PÚBLICOS
    PROCEDURE SP_RET_FOLHA_1207(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);

    PROCEDURE SP_1210_RENDIMENTOS(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);

    PROCEDURE SP_1299_FECHAMENTO_PE(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);

    PROCEDURE SP_1298_REABERTURA_PE(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE);

    PROCEDURE SP_ALT_1207_INDIVIDUAL(i_id_cad_folha in     esocial.tsoc_cad_folha.id_cad_folha%type,
                                     o_retorno      in out varchar2);

    PROCEDURE SP_ALT_1210_INDIVIDUAL(i_id_cad_folha in     esocial.tsoc_cad_folha.id_cad_folha%type,
                                     o_retorno      in out varchar2);

    PROCEDURE SP_ALT_1299_INDIVIDUAL(i_id_cad_fechamento in     esocial.tsoc_cad_fechamento_ep.id_cad_fechamento%type,
                                     o_retorno           in out varchar2);


end PAC_ESOCIAL_EVENTOS_PE_102;

/


CREATE OR REPLACE PACKAGE BODY         PAC_ESOCIAL_EVENTOS_PE_102 IS

    --Log de Erros
    GB_REC_ERRO ESOCIAL.TSOC_CTR_ERRO_PROCESSO%ROWTYPE;

    --Id do processo
    GB_ID_CTR_PROCESSO ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE;

    --Id da Origem
    GB_ID_ORIGEM ESOCIAL.TSOC_PAR_ORIGEM.ID_ORIGEM%TYPE;
    --Id do evento
    GB_ID_EVENTO ESOCIAL.TSOC_PAR_EVENTO.ID_EVENTO%TYPE;
    --Id do período
    GB_ID_PERIODO_DET ESOCIAL.TSOC_CTR_PERIODO_DET.ID_PERIODO_DET%TYPE;
    --Cod Ins
    GB_COD_INS NUMBER;
    --Per_Processo
    --GB_PER_PROCESSO DATE;
    GB_PER_COMPETENCIA DATE;
    --Faixa Processamento
    GB_FAIXA_INI NUMBER;
    GB_FAIXA_FIM NUMBER;
    GB_FAIXA_INI_CPF VARCHAR2(11);
    GB_FAIXA_FIM_CPF VARCHAR2(11);
    
    --ID APURACAO --TT83733
    GB_ID_APURACAO TSOC_CTR_PERIODO.ID_APURACAO%TYPE;     
    
    --INDRETIF --TT84697
    GB_IND_RETIF TSOC_1207_BENEFICIO.INDRETIF%TYPE;     
    GB_NR_RECIBO TSOC_1207_BENEFICIO.WS_NUM_RECIBO%TYPE;   


    TYPE GB_TY_FOLHA IS RECORD (
        COD_IDE_CLI VARCHAR2(20),
        COD_BENEFICIO NUMBER(8),
        SEQ_PAGAMENTO NUMBER,
        TIP_PROCESSO VARCHAR(1),
        PER_PROCESSO DATE,
        PER_COMPETENCIA DATE );

    GB_FOLHA GB_TY_FOLHA;

    -- v1.02 - início
    /*
    --Controle de intervalo de tempo para geração de ID
    GB_DAT_EVT_ATU  VARCHAR2(100);
    GB_DAT_EVT_ANT  VARCHAR2(100);
    GB_SEQ_CHAVE_ID NUMBER;
    */
    -- v1.02 - fim

    --Dados do ambiente
    TYPE GB_TY_AMB IS RECORD(
    TPAMB   TSOC_PAR_AMBIENTE.TPAMB%TYPE,
    PROCEMI TSOC_PAR_AMBIENTE.PROCEMI%TYPE,
    VERPROC TSOC_PAR_AMBIENTE.VERPROC%TYPE);

    GB_AMB GB_TY_AMB;

    --Dados do Empregador
    TYPE GB_TY_EMPREGADOR IS RECORD(
    TP_INSC  ESOCIAL.TSOC_CAD_EMPREGADOR.TPINSC%TYPE,
    NUM_CNPJ ESOCIAL.TSOC_CAD_EMPREGADOR.NRINSC%TYPE);

    GB_EMPREGADOR GB_TY_EMPREGADOR;

    --Cursor Cadastro de Folha
    CURSOR C_CAD_FOLHA(P_FAIXA_INI IN VARCHAR2, P_FAIXA_FIM IN VARCHAR2) IS
        SELECT DISTINCT F.COD_INS,
                        F.COD_IDE_CLI,
                        F.PER_PROCESSO,
                        F.PER_COMPETENCIA,--TT82342
                        PF.NUM_CPF  --v1.02
        FROM USER_IPESP.TB_ESOCIAL_HFOLHA F,
             USER_IPESP.TB_PESSOA_FISICA PF
        WHERE F.COD_INS      = GB_COD_INS
          --Considera como período o periodo em aberto do esocial relacionado ao processo.
          AND F.PER_PROCESSO = GB_PER_COMPETENCIA--GB_PER_PROCESSO
          --DALVES EXCLUIR
          --AND F.PER_PROCESSO = TO_DATE('01/08/2022','DD/MM/YYYY')
          --TT82342 - Folha de Recadastramento e Suplementar
          --and pf.cod_ide_cli = '010000254820300'
          --
          AND F.PER_COMPETENCIA = GB_PER_COMPETENCIA
          AND F.COD_IDE_CLI  = PF.COD_IDE_CLI
          AND F.COD_INS      = PF.COD_INS
          --FAIXA
          AND PF.NUM_CPF >= nvl(P_FAIXA_INI,PF.NUM_CPF)
          AND PF.NUM_CPF <= nvl(P_FAIXA_FIM,PF.NUM_CPF)
          /*--ATIVOS E SUSPENSOS
          -- comentado por ljunior em 02/09/2022 : Temos que enviar tudo que foi pago no mês
          AND EXISTS (SELECT 1
                      FROM USER_IPESP.TB_BENEFICIARIO BEN
                      WHERE BEN.COD_INS = PF.COD_INS
                        AND BEN.COD_IDE_CLI_BEN = PF.COD_IDE_CLI
                        AND BEN.FLG_STATUS IN ('A', 'S')
                        --NOVO CRITÉRIO
                        AND (BEN.DAT_FIM_BEN IS NULL OR BEN.DAT_FIM_BEN > = SYSDATE)
                        AND BEN.PER_ULT_PROCESSO >= '01/06/2009'
                        AND BEN.COD_PROC_GRP_PAG NOT IN (80, 82))*/
          AND NOT EXISTS (SELECT 1
                          FROM esocial.TSOC_CAD_FOLHA C
                          WHERE C.COD_INS      = F.COD_INS
                            AND C.COD_IDE_CLI  = F.COD_IDE_CLI
                            AND C.PER_PROCESSO = F.PER_PROCESSO
                            --TT82342 - Folha de Recadastramento e Suplementar
                            AND C.PER_COMPETENCIA = F.PER_COMPETENCIA
                            --TT83733
                            AND C.ID_APURACAO = GB_ID_APURACAO)
          --TT83917 - Melhoria na Extração do Evento (Rubricas Faltantes)                   
          AND EXISTS (SELECT 1
            FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO A,
                 ESOCIAL.TSOC_CAD_RUBRICA     C
            WHERE A.COD_INS             = F.COD_INS
              AND A.TIP_PROCESSO        = F.TIP_PROCESSO
              AND A.SEQ_PAGAMENTO       = F.SEQ_PAGAMENTO
              AND A.PER_PROCESSO        = F.PER_PROCESSO
              AND A.COD_BENEFICIO       = F.COD_BENEFICIO
              AND A.COD_IDE_CLI         = F.COD_IDE_CLI
              AND TO_CHAR(A.DAT_INI_REF, 'MM/YYYY') = TO_CHAR(A.PER_PROCESSO, 'MM/YYYY')
              AND A.COD_INS = C.COD_INS
              AND TO_CHAR(A.COD_FCRUBRICA)         = C.COD_RUBRICA
         );

    CURSOR C_CAD_DET_FOLHA(P_COD_IDE_CLI   IN USER_IPESP.TB_HFOLHA.COD_IDE_CLI%TYPE) IS
        SELECT F.TIP_PROCESSO,
               F.SEQ_PAGAMENTO,
               F.DT_FECHAMENTO,
               F.NUM_GRP,
               F.VAL_SAL_BASE,
               F.TOT_CRED,
               F.TOT_DEB,
               F.VAL_LIQUIDO,
               F.COD_ENTIDADE,
               --TT78443 - Esocial SPPREV S-2410 / S-1207: Conteúdo Inválido: Atualizar o NrBenefício
               P.NUM_CPF||F.COD_BENEFICIO AS NRBENEFICIO,
               F.COD_BENEFICIO,
               F.PER_PROCESSO
        FROM USER_IPESP.TB_ESOCIAL_HFOLHA F, 
             USER_IPESP.TB_PESSOA_FISICA P
        WHERE F.COD_INS      = P.COD_INS
          AND F.COD_IDE_CLI  = P.COD_IDE_CLI
         -- AND F.PER_PROCESSO = GB_PER_PROCESSO
         --DALVES EXCLUIR
          --AND F.PER_PROCESSO = TO_DATE('01/08/2022','DD/MM/YYYY')
          AND F.PER_COMPETENCIA = GB_PER_COMPETENCIA
          AND F.COD_INS      = GB_COD_INS
          AND F.COD_IDE_CLI  = P_COD_IDE_CLI;
    
    --Cursor de retificações da Folha
    CURSOR C_RET_FOLHA(P_FAIXA_INI IN VARCHAR2, P_FAIXA_FIM IN VARCHAR2) IS
        SELECT DISTINCT R.COD_INS,
                        F.COD_IDE_CLI,
                        F.PER_PROCESSO,
                        F.PER_COMPETENCIA,
                        F.NUM_CPF,
                        F.ID_CAD_FOLHA,
                        F.ID_APURACAO,
                        R.NR_RECIBO
        FROM ESOCIAL.TSOC_CTR_RETIFICACAO R, ESOCIAL.TSOC_CAD_FOLHA F
        WHERE R.COD_INS = F.COD_INS
        AND R.ID_CAD_FOLHA = F.ID_CAD_FOLHA;
          


    PROCEDURE SP_SET_PER_PROCESSO IS
        --V_PER_PROCESSO DATE;
        V_PER_COMPETENCIA DATE;
    BEGIN

        SELECT TO_DATE(P.PERIODO, 'MM/YYYY')
        --INTO V_PER_PROCESSO
        INTO V_PER_COMPETENCIA
        FROM ESOCIAL.TSOC_CTR_PERIODO_DET PD, ESOCIAL.TSOC_CTR_PERIODO P
        WHERE P.COD_INS         = GB_COD_INS
          AND P.ID_PERIODO      = PD.ID_PERIODO
          AND PD.ID_PERIODO_DET = GB_ID_PERIODO_DET;

        --GB_PER_PROCESSO := V_PER_PROCESSO;
        GB_PER_COMPETENCIA := V_PER_COMPETENCIA;

    END SP_SET_PER_PROCESSO;


    PROCEDURE SP_RET_INFO_AMBIENTE IS
    BEGIN
        SELECT PA.TPAMB, PA.PROCEMI, PA.VERPROC
        INTO GB_AMB --  P_TPAMB, P_PROCEMI, P_VERPROC
        FROM ESOCIAL.TSOC_PAR_AMBIENTE PA
        WHERE PA.FLG_STATUS = 'A';
    END SP_RET_INFO_AMBIENTE;


    PROCEDURE SP_DEFAULT_SESSION IS
    BEGIN

        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_LANGUAGE =  ''BRAZILIAN PORTUGUESE''';
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TERRITORY = ''BRAZIL''';
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_LANGUAGE = ''BRAZILIAN PORTUGUESE''';
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''DD/MM/YYYY''';
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,''';
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_CURRENCY = ''R$''';
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_CALENDAR = ''GREGORIAN''';
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_ISO_CURRENCY  = ''BRAZIL''';

    END SP_DEFAULT_SESSION;


    PROCEDURE SP_SETA_PROCESSO(P_NOM_EVENTO IN VARCHAR2) IS
    BEGIN

        IF P_NOM_EVENTO = 'INICIO_PROCESSAMENTO' THEN

            UPDATE ESOCIAL.TSOC_CTR_PROCESSO
            SET DAT_INICIO      = SYSDATE,
                DAT_FIM         = NULL,
                FLG_STATUS      = 'P',
                DAT_ULT_ATU     = SYSDATE,
                NOM_USU_ULT_ATU = 'ESOCIAL',
                NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
            WHERE ID_CTR_PROCESSO = GB_ID_CTR_PROCESSO;

            -- COMMIT;  -- v1.02

        ELSIF P_NOM_EVENTO = 'FIM_PROCESSAMENTO' THEN

            UPDATE ESOCIAL.TSOC_CTR_PROCESSO
            SET DAT_FIM         = SYSDATE,
                FLG_STATUS      = 'F',
                DAT_ULT_ATU     = SYSDATE,
                NOM_USU_ULT_ATU = 'ESOCIAL',
                NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
            WHERE ID_CTR_PROCESSO = GB_ID_CTR_PROCESSO;

            -- COMMIT;  -- v1.02

        ELSIF P_NOM_EVENTO = 'ATUALIZA_QUANTIDADE' THEN

            --ATUALIZACAO DE QUANTIDADE DE REGISTROS
            UPDATE ESOCIAL.TSOC_CTR_PROCESSO
            SET QTD_REGISTROS   = NVL(QTD_REGISTROS, 0) + 1,
                DAT_ULT_ATU     = SYSDATE,
                NOM_USU_ULT_ATU = 'ESOCIAL',
                NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
            WHERE ID_CTR_PROCESSO = GB_ID_CTR_PROCESSO;

            -- COMMIT;  -- v1.02

        ELSE

            --ERRO NO PROCESSAMENTO
            UPDATE ESOCIAL.TSOC_CTR_PROCESSO
            SET FLG_STATUS      = 'E',
                DAT_ULT_ATU     = SYSDATE,
                NOM_USU_ULT_ATU = 'ESOCIAL',
                NOM_PRO_ULT_ATU = 'SP_SETA_PROCESSO'
            WHERE ID_CTR_PROCESSO = GB_ID_CTR_PROCESSO;

            -- COMMIT;  -- v1.02

        END IF;

    END SP_SETA_PROCESSO;


    /*
    PROCEDURE SP_GERA_ERRO_PROCESSO IS
    V_ID_CAD_ERRO ESOCIAL.TSOC_CTR_ERRO_PROCESSO.ID_ERRO%TYPE;
    BEGIN

        V_ID_CAD_ERRO := ESOCIAL.ESOC_SEQ_ID_ERRO_PROCESSO.NEXTVAL;

        INSERT INTO ESOCIAL.TSOC_CTR_ERRO_PROCESSO
          (ID_ERRO,
           COD_INS,
           ID_CAD,
           NOM_PROCESSO,
           ID_EVENTO,
           DESC_ERRO,
           DAT_ING,
           DAT_ULT_ATU,
           NOM_USU_ULT_ATU,
           NOM_PRO_ULT_ATU,
           DESC_ERRO_BD,
           DES_IDENTIFICADOR,
           FLG_TIPO_ERRO,
           ID_CTR_PROCESSO,
           DET_ERRO)
        VALUES
          (V_ID_CAD_ERRO,
           GB_REC_ERRO.COD_INS,
           GB_REC_ERRO.ID_CAD,
           GB_REC_ERRO.NOM_PROCESSO,
           GB_REC_ERRO.ID_EVENTO,
           GB_REC_ERRO.DESC_ERRO,
           SYSDATE,
           SYSDATE,
           'ESOCIAL',
           'SP_GERA_ERRO_PROCESSO',
           GB_REC_ERRO.DESC_ERRO_BD,
           GB_REC_ERRO.DES_IDENTIFICADOR,
           GB_REC_ERRO.FLG_TIPO_ERRO,
           GB_ID_CTR_PROCESSO,
           GB_REC_ERRO.DET_ERRO);

    END SP_GERA_ERRO_PROCESSO;
    */


    PROCEDURE SP_GERA_ERRO_PROCESSO_AT (i_rec_erro in tsoc_ctr_erro_processo%rowtype) IS

        PRAGMA AUTONOMOUS_TRANSACTION;

    BEGIN

        insert into esocial.tsoc_ctr_erro_processo
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
        values
            (esocial.esoc_seq_id_erro_processo.nextval,
             i_rec_erro.cod_ins,
             i_rec_erro.id_cad,
             i_rec_erro.nom_processo,
             i_rec_erro.id_evento,
             i_rec_erro.desc_erro,
             sysdate,
             sysdate,
             'ESOCIAL',
             'SP_GERA_ERRO_PROCESSO_AT',
             i_rec_erro.desc_erro_bd,
             i_rec_erro.des_identificador,
             i_rec_erro.flg_tipo_erro,
             i_rec_erro.id_ctr_processo,
             i_rec_erro.det_erro);

        COMMIT;

    END SP_GERA_ERRO_PROCESSO_AT;


     PROCEDURE SP_CARREGA_IDS IS
     BEGIN

        SELECT B.ID_ORIGEM, B.ID_EVENTO, C.ID_PERIODO_DET, C.COD_INS, A.FAIXA_INI, A.FAIXA_FIM,A.NUM_CPF_INICIAL, A.NUM_CPF_FINAL, d.id_apuracao
        INTO GB_ID_ORIGEM, GB_ID_EVENTO, GB_ID_PERIODO_DET, GB_COD_INS, GB_FAIXA_INI, GB_FAIXA_FIM, GB_FAIXA_INI_CPF, GB_FAIXA_FIM_CPF, gb_id_apuracao--TT83733 

        FROM ESOCIAL.TSOC_CTR_PROCESSO    A,
             ESOCIAL.TSOC_PAR_PROCESSO    B,
             ESOCIAL.TSOC_CTR_PERIODO_DET C,
             ESOCIAL.TSOC_CTR_PERIODO     D
        WHERE A.COD_INS = B.COD_INS
          AND A.ID_PROCESSO = B.ID_PROCESSO
          AND A.COD_INS = C.COD_INS
          AND A.ID_PERIODO = C.ID_PERIODO
          AND B.COD_INS = C.COD_INS
          AND B.ID_ORIGEM = C.ID_ORIGEM
          AND B.ID_EVENTO = C.ID_EVENTO
          AND A.ID_CTR_PROCESSO = GB_ID_CTR_PROCESSO
          AND D.ID_PERIODO = C.ID_PERIODO
          AND D.COD_INS = C.COD_INS
          AND B.FLG_STATUS = 'A' --PROCESSO COM STATUS ATIVO
          AND A.FLG_STATUS = 'A' --COM STATUS AGENDADO
          AND C.FLG_STATUS IN ('A', 'R') --PERÍODO ABERTO OU REABERTO PARA O EVENTO
          AND D.FLG_STATUS IN ('A', 'R'); --PERIODO ABERTO OU REABERTO

    END SP_CARREGA_IDS;


    PROCEDURE SP_CARREGA_IDS_EVENTO(i_cod_evento in tsoc_par_evento.cod_evento%type) IS
    BEGIN

        -- Obtém o período aberto ou reaberto para o id do evento
        select b.id_origem, b.id_evento, b.id_periodo_det, b.cod_ins
        into gb_id_origem, gb_id_evento, gb_id_periodo_det, gb_cod_ins
        from tsoc_par_evento a,
             tsoc_ctr_periodo_det b
        where a.cod_evento = i_cod_evento
          and b.flg_status in ('A', 'R') -- período aberto ou reaberto
          and b.cod_ins    = a.cod_ins
          and b.id_evento  = a.id_evento;

    END SP_CARREGA_IDS_EVENTO;


    PROCEDURE SP_RET_INSC_EMP IS
    BEGIN

        SELECT EMP.NRINSC, EMP.TPINSC
        INTO GB_EMPREGADOR.NUM_CNPJ, GB_EMPREGADOR.TP_INSC
        FROM ESOCIAL.TSOC_CAD_EMPREGADOR  EMP,
             ESOCIAL.TSOC_CTR_PERIODO_DET PD,
             ESOCIAL.TSOC_PAR_ORIGEM      PO
        WHERE EMP.COD_INS       = GB_COD_INS
          AND PD.ID_ORIGEM      = PO.ID_ORIGEM
          AND PO.ID_EMPREGADOR  = EMP.ID_EMPREGADOR
          AND PD.COD_INS        = EMP.COD_INS
          AND PD.ID_PERIODO_DET = GB_ID_PERIODO_DET
          AND PD.ID_ORIGEM      = GB_ID_ORIGEM;

    EXCEPTION
        WHEN OTHERS THEN
            GB_EMPREGADOR.TP_INSC  := NULL;
            GB_EMPREGADOR.NUM_CNPJ := NULL;

    END SP_RET_INSC_EMP;


    FUNCTION FC_GERA_ID_EVENTO RETURN VARCHAR2 IS
    BEGIN

        -- v1.01 - Início
        --RETURN 'ID' || GB_EMPREGADOR.TP_INSC || GB_EMPREGADOR.NUM_CNPJ || GB_DAT_EVT_ATU || LPAD(GB_SEQ_CHAVE_ID, 5, 0);
        RETURN 'ID' || GB_EMPREGADOR.TP_INSC || rpad(GB_EMPREGADOR.NUM_CNPJ,14,'0') || fnc_seq_id_evento;
        -- V1.01 - Fim

    END FC_GERA_ID_EVENTO;


    PROCEDURE SP_INC_CAD_FOLHA(P_CAD_FOLHA IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CAD_FOLHA
            (ID_CAD_FOLHA,
             COD_INS,
             COD_IDE_CLI,
             PER_PROCESSO,
             PER_COMPETENCIA,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU,
             NUM_CPF,
             ID_ORIGEM,
             --TT83733
             ID_APURACAO)
        VALUES
            (P_CAD_FOLHA.ID_CAD_FOLHA,
             P_CAD_FOLHA.COD_INS,
             P_CAD_FOLHA.COD_IDE_CLI,
             P_CAD_FOLHA.PER_PROCESSO,
             P_CAD_FOLHA.PER_COMPETENCIA,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_CAD_FOLHA',
             P_CAD_FOLHA.NUM_CPF,
             GB_ID_ORIGEM,
             --TT83733
             GB_ID_APURACAO);

    END SP_INC_CAD_FOLHA;


    PROCEDURE SP_INC_CAD_DET_FOLHA(P_CAD_DET_FOLHA ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CAD_DET_FOLHA
            (ID_DET_CAD,
             ID_CAD_FOLHA,
             TIP_PROCESSO,
             SEQ_PAGAMENTO,
             DT_FECHAMENTO,
             NUM_GRP,
             VAL_SAL_BASE,
             TOT_CRED,
             TOT_DEB,
             VAL_LIQUIDO,
             COD_ENTIDADE,
             COD_BENEFICIO,
             PER_PROCESSO,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_CAD_DET_FOLHA.ID_DET_CAD,
             P_CAD_DET_FOLHA.ID_CAD_FOLHA,
             P_CAD_DET_FOLHA.TIP_PROCESSO,
             P_CAD_DET_FOLHA.SEQ_PAGAMENTO,
             P_CAD_DET_FOLHA.DT_FECHAMENTO,
             P_CAD_DET_FOLHA.NUM_GRP,
             P_CAD_DET_FOLHA.VAL_SAL_BASE,
             P_CAD_DET_FOLHA.TOT_CRED,
             P_CAD_DET_FOLHA.TOT_DEB,
             P_CAD_DET_FOLHA.VAL_LIQUIDO,
             P_CAD_DET_FOLHA.COD_ENTIDADE,
             P_CAD_DET_FOLHA.COD_BENEFICIO,
             P_CAD_DET_FOLHA.PER_PROCESSO,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_CAD_DET_FOLHA');

    END SP_INC_CAD_DET_FOLHA;


    FUNCTION FC_CPF_BEN(P_COD_IDE_CLI IN ESOCIAL.TSOC_CAD_FOLHA.COD_IDE_CLI%TYPE)
            RETURN ESOCIAL.TSOC_1207_BENEFICIO.CPFBENEF%TYPE IS

        V_NUM_CPF USER_IPESP.TB_PESSOA_FISICA.NUM_CPF%TYPE;

    BEGIN
        SELECT NUM_CPF
        INTO V_NUM_CPF
        FROM USER_IPESP.TB_PESSOA_FISICA PF
        WHERE PF.COD_INS = GB_COD_INS
          AND PF.COD_IDE_CLI = P_COD_IDE_CLI;

        RETURN V_NUM_CPF;

    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;

    END FC_CPF_BEN;

    PROCEDURE SP_INC_1207_BENEFICIO(P_1207_BENEFICIO IN ESOCIAL.TSOC_1207_BENEFICIO%ROWTYPE) IS
    BEGIN

        INSERT INTO TSOC_1207_BENEFICIO
          (ID_PK,
           ID_CAD_FOLHA,
           ID_ORIGEM,
           ID_LOTE,
           ID_EVENTO,
           SEQ_EVENTO,
           ID,
           INDRETIF,
           NRRECIBO,
           INDAPURACAO,
           PERAPUR,
           TPAMB,
           PROCEMI,
           VERPROC,
           TPINSC,
           NRINSC,
           CPFBENEF,
           CTR_FLG_STATUS,
           XML_ENVIO,
           WS_COD_RESPOSTA,
           WS_DSC_RESPOSTA,
           WS_DH_PROC,
           WS_VER_APP_PROC,
           FLG_VIGENCIA,
           DAT_ING,
           DAT_ULT_ATU,
           NOM_USU_ULT_ATU,
           NOM_PRO_ULT_ATU,
           ID_PERIODO_DET,
           COD_INS)
        VALUES
          (P_1207_BENEFICIO.ID_PK,
           P_1207_BENEFICIO.ID_CAD_FOLHA,
           P_1207_BENEFICIO.ID_ORIGEM,
           P_1207_BENEFICIO.ID_LOTE,
           P_1207_BENEFICIO.ID_EVENTO,
           P_1207_BENEFICIO.SEQ_EVENTO,
           P_1207_BENEFICIO.ID,
           P_1207_BENEFICIO.INDRETIF,
           P_1207_BENEFICIO.NRRECIBO,
           P_1207_BENEFICIO.INDAPURACAO,
           P_1207_BENEFICIO.PERAPUR,
           P_1207_BENEFICIO.TPAMB,
           P_1207_BENEFICIO.PROCEMI,
           P_1207_BENEFICIO.VERPROC,
           P_1207_BENEFICIO.TPINSC,
           P_1207_BENEFICIO.NRINSC,
           P_1207_BENEFICIO.CPFBENEF,
           P_1207_BENEFICIO.CTR_FLG_STATUS,
           P_1207_BENEFICIO.XML_ENVIO,
           P_1207_BENEFICIO.WS_COD_RESPOSTA,
           P_1207_BENEFICIO.WS_DSC_RESPOSTA,
           P_1207_BENEFICIO.WS_DH_PROC,
           P_1207_BENEFICIO.WS_VER_APP_PROC,
           P_1207_BENEFICIO.FLG_VIGENCIA,
           SYSDATE,
           SYSDATE,
           USER,
           'SP_IN_1207_BENEFICIO',
           P_1207_BENEFICIO.ID_PERIODO_DET,
           P_1207_BENEFICIO.COD_INS);

    END SP_INC_1207_BENEFICIO;
    
   --TICKET 84697 - DALVES - 18/01/2023
   PROCEDURE SP_INC_H1207_BENEFICIO(P_ID_CAD_FOLHA IN ESOCIAL.TSOC_1207_BENEFICIO.ID_CAD_FOLHA%TYPE) IS
    BEGIN
      INSERT INTO TSOC_H1207_BENEFICIO
      SELECT b.*, '' FROM TSOC_1207_BENEFICIO b WHERE b.ID_CAD_FOLHA = P_ID_CAD_FOLHA;

    END SP_INC_H1207_BENEFICIO; 

   --TICKET 84697 - DALVES - 18/01/2023
   PROCEDURE SP_DEL_1207_BENEFICIO(P_ID_CAD_FOLHA IN ESOCIAL.TSOC_1207_BENEFICIO.ID_CAD_FOLHA%TYPE) IS
     v_id_pk               esocial.tsoc_1207_beneficio.id_pk%type;
     /*v_id_demonstrativo    esocial.tsoc_cpl_1207_demonstrativo.id_demonstrativo%type;
     v_id_unidade_n        esocial.tsoc_cpl_1207_orgao_unidade_n.id_unidade_n%type;
     v_id_proc_retroativo  esocial.tsoc_cpl_1207_proc_retroativo.id_proc_retroativo%type;
     v_id_retroativo       esocial.tsoc_cpl_1207_retroativo.id_retroativo%type;
     v_id_org_unidade_r    esocial.tsoc_cpl_1207_orgao_unidade_r.id_org_unidade_r%type;*/
    BEGIN
      
      --busca chave
      FOR REG IN (
        select b.id_pk,
               d.id_demonstrativo,
               o.id_unidade_n,
               p.id_proc_retroativo,
               ro.id_retroativo,
               ro.id_org_unidade_r
          from esocial.tsoc_1207_beneficio b,
               esocial.tsoc_cpl_1207_demonstrativo d,
               esocial.tsoc_cpl_1207_orgao_unidade_n o,
               esocial.tsoc_cpl_1207_proc_retroativo p,
               (select r.id_proc_retroativo, r.id_retroativo, oo.id_org_unidade_r
                  from esocial.tsoc_cpl_1207_retroativo      r,
                       esocial.tsoc_cpl_1207_orgao_unidade_r oo
                 where oo.id_retroativo = r.id_retroativo) ro
         where b.id_pk = d.id_pk
           and d.id_demonstrativo = o.id_demonstrativo
           and d.id_demonstrativo = p.id_demonstrativo(+)
           and p.id_proc_retroativo = ro.id_proc_retroativo(+)
           and b.id_cad_folha = P_ID_CAD_FOLHA) LOOP
      
      --delete retroativo
      delete from esocial.tsoc_cpl_1207_rubrica_r where id_org_unidade_r = reg.id_org_unidade_r;
      delete from esocial.tsoc_cpl_1207_orgao_unidade_r where id_retroativo = reg.id_retroativo;
      delete from esocial.tsoc_cpl_1207_retroativo where id_proc_retroativo = reg.id_proc_retroativo;
      delete from esocial.tsoc_cpl_1207_proc_retroativo where id_demonstrativo = reg.id_demonstrativo;
      --delete folha normal
      delete from esocial.tsoc_cpl_1207_rubrica_n where id_unidade_n = reg.id_unidade_n;
      delete from esocial.tsoc_cpl_1207_orgao_unidade_n where id_demonstrativo = reg.id_demonstrativo;
      v_id_pk := reg.id_pk;
    END LOOP;
    --DELETE TSOC 1207
    delete from esocial.tsoc_cpl_1207_demonstrativo where id_pk = v_id_pk; 
    delete from esocial.tsoc_1207_beneficio where id_cad_folha = P_ID_CAD_FOLHA;
    
    END SP_DEL_1207_BENEFICIO;       

    PROCEDURE SP_INC_1207_DEM(P_1207_DEM IN ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO%ROWTYPE) IS
    BEGIN
        INSERT INTO ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO
          (ID_DEMONSTRATIVO,
           ID_PK,
           IDEMDEV,
           NRBENEFICIO,
           DAT_ING,
           DAT_ULT_ATU,
           NOM_USU_ULT_ATU,
           NOM_PRO_ULT_ATU,
           SEQ_PAGAMENTO,
           PERREF)
        VALUES
          (P_1207_DEM.ID_DEMONSTRATIVO,
           P_1207_DEM.ID_PK,
           P_1207_DEM.IDEMDEV,
           P_1207_DEM.NRBENEFICIO,
           SYSDATE,
           SYSDATE,
           USER,
           'SP_INC_1207_DEM',
           P_1207_DEM.SEQ_PAGAMENTO,
           P_1207_DEM.PERREF);

        -- COMMIT;  -- v1.02

    END SP_INC_1207_DEM;

  FUNCTION FC_GET_CNPJ_ENT(P_COD_BENEFICIO IN USER_IPESP.TB_CONCESSAO_BENEFICIO.COD_BENEFICIO%TYPE)
    RETURN ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_N.NRINSC%TYPE IS
    V_CNPJ_ENT ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_N.NRINSC%TYPE;
  BEGIN

    SELECT E.NUM_CNPJ
      INTO V_CNPJ_ENT
      FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB,
           USER_IPESP.TB_ENTIDADE E
     WHERE CB.COD_ENTIDADE = E.COD_ENTIDADE
       AND CB.COD_BENEFICIO = P_COD_BENEFICIO;

    RETURN V_CNPJ_ENT;

  END FC_GET_CNPJ_ENT;

    PROCEDURE SP_INC_ORG_UNIDADE_N(P_ORG_UNI_N IN ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_N%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_N
            (ID_UNIDADE_N,
             ID_DEMONSTRATIVO,
             TPINSC,
             NRINSC,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_ORG_UNI_N.ID_UNIDADE_N,
             P_ORG_UNI_N.ID_DEMONSTRATIVO,
             P_ORG_UNI_N.TPINSC,
             P_ORG_UNI_N.NRINSC,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_ORG_UNIDADE');

        -- COMMIT; -- v1.02

    END SP_INC_ORG_UNIDADE_N;

    PROCEDURE SP_INC_1207_RUBRICA_N(P_RUBRICAS_N IN ESOCIAL.TSOC_CPL_1207_RUBRICA_N%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1207_RUBRICA_N
            (ID_RUBRICA_N,
             ID_UNIDADE_N,
             CODRUBR,
             IDETABRUBR,
             QTDRUBR,
             FATORRUBR,
             VRUNIT,
             VRRUBR,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_RUBRICAS_N.ID_RUBRICA_N,
             P_RUBRICAS_N.ID_UNIDADE_N,
             P_RUBRICAS_N.CODRUBR,
             P_RUBRICAS_N.IDETABRUBR,
             P_RUBRICAS_N.QTDRUBR,
             P_RUBRICAS_N.FATORRUBR,
             P_RUBRICAS_N.VRUNIT,
             P_RUBRICAS_N.VRRUBR,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_1207_RUBRICA_N');

        -- COMMIT; -- v1.02

    END SP_INC_1207_RUBRICA_N;

    PROCEDURE SP_1207_RUBRICAS_N(P_CAD_FOLHA     IN     ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                                 P_CAD_DET_FOLHA IN     ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE,
                                 P_ID_UNIDADE_N  IN     ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_N.ID_UNIDADE_N%TYPE) IS

        V_CPL_RUBRICAS_N ESOCIAL.TSOC_CPL_1207_RUBRICA_N%ROWTYPE;

        CURSOR C_RUBRICA_N(P_CAD_FOLHA     IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                           P_CAD_DET_FOLHA IN ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE) IS

            SELECT D.IDERUBRICA_CODRUBR AS CODRUBR,
                   D.IDERUBRICA_IDETABRUBR AS IDETABRUBR,
                   -- TT77896 - DALVES - 04/05/2022   
                   -- LJUNIOR EM 05/09/2022 PEGA DIRETO DA HDET
                   CASE WHEN B.TIP_COMPOSICAO = 'B' THEN DECODE(TRUNC(A.VAL_INIDADE, 2),0,NULL,TRUNC(A.VAL_INIDADE, 2)) ELSE NULL END AS QTDRUBR,
                   /*(SELECT --DECODE(to_char(TRUNC(E.VAL_INIDADE, 2),'FM999999999999,90'),'0,00',NULL,to_char(TRUNC(E.VAL_INIDADE, 2),'FM999999999999,90'))
                   DECODE(TRUNC(E.VAL_INIDADE, 2),0,NULL,TRUNC(E.VAL_INIDADE, 2))
                    FROM USER_IPESP.TB_COMPOSICAO_BEN E
                    WHERE E.COD_BENEFICIO = A.COD_BENEFICIO
                      AND E.COD_FCRUBRICA = A.COD_FCRUBRICA
                      AND B.TIP_COMPOSICAO = 'B'
                      AND ROWNUM = 1)   AS QTDRUBR,*/
                   -- TT77894 - DALVES - 04/05/2022  
                   -- LJUNIOR EM 05/09/2022 PEGA DIRETO DA HDET
                   CASE WHEN B.TIP_COMPOSICAO = 'B' AND LENGTH(TRUNC(A.VAL_PORC)) <= 5 
                        THEN DECODE(TRUNC(A.VAL_PORC, 2),0,NULL,TRUNC(A.VAL_PORC, 2)) 
                        ELSE NULL 
                    END AS FATORRUBR,
                   /*(SELECT  --DECODE(to_char(TRUNC(E.VAL_PORC, 2),'FM999999999999,90'),'0,00',NULL,to_char(TRUNC(E.VAL_PORC, 2),'FM999999999999,90'))
                    DECODE(TRUNC(E.VAL_PORC, 2),0,NULL,TRUNC(E.VAL_PORC, 2))
                    FROM USER_IPESP.TB_COMPOSICAO_BEN E 
                    WHERE E.COD_BENEFICIO = A.COD_BENEFICIO
                      AND E.COD_FCRUBRICA = A.COD_FCRUBRICA
                      AND B.TIP_COMPOSICAO = 'B'
                      AND ROWNUM = 1)   AS FATORRUBR,*/
                   TRUNC(A.VAL_RUBRICA, 2)        AS VRUNIT,
                   TRUNC(A.VAL_RUBRICA, 2)       AS VRRUBR,
                   TO_CHAR(A.DAT_INI_REF, 'MM/YYYY'),
                   TO_CHAR(A.PER_PROCESSO, 'MM/YYYY')
            FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO A,
                 USER_IPESP.TB_RUBRICAS       B,
                 ESOCIAL.TSOC_CAD_RUBRICA     C,
                 ESOCIAL.TSOC_1010_RUBRICA    D,
                 USER_IPESP.TB_CONCESSAO_BENEFICIO CC
            WHERE A.COD_IDE_CLI         = P_CAD_FOLHA.COD_IDE_CLI
              AND A.COD_INS             = P_CAD_FOLHA.COD_INS
              AND A.COD_BENEFICIO       = P_CAD_DET_FOLHA.COD_BENEFICIO
              AND A.PER_PROCESSO        = P_CAD_FOLHA.PER_PROCESSO
              AND A.SEQ_PAGAMENTO       = P_CAD_DET_FOLHA.SEQ_PAGAMENTO
              AND A.TIP_PROCESSO        = P_CAD_DET_FOLHA.TIP_PROCESSO
                 --Normal
              AND TO_CHAR(A.DAT_INI_REF, 'MM/YYYY') = TO_CHAR(A.PER_PROCESSO, 'MM/YYYY')
              AND B.COD_RUBRICA         = A.COD_FCRUBRICA
              --AND B.COD_RUBRICA         = C.COD_RUBRICA
              AND B.TIP_EVENTO_ESPECIAL <> 'P'
              AND B.COD_ENTIDADE = CC.COD_ENTIDADE
              -- LJUNIOR EM 02/09/2022 -- COMENTADO POR ORIRNTACAO DO CARLOS -- NAO DEVE RESTRINGIR RUBRICAS DE IR
              -- AND TRUNC(B.COD_RUBRICA / 100) NOT IN (70012, 70014, 70078, 70081)
              AND CC.COD_INS = 1
              AND CC.COD_BENEFICIO = A.COD_BENEFICIO
              AND B.COD_ENTIDADE = CC.COD_ENTIDADE                             
              AND D.ID_CAD_RUB          = C.ID_CAD_RUB
              AND D.COD_INS             = C.COD_INS
              AND D.FLG_VIGENCIA        = 'A'
              AND D.ID_ORIGEM           = 1
              AND TO_CHAR(B.COD_RUBRICA)         = C.COD_RUBRICA
              --TICKET 79609 - Esocial SPPREV S-1207: Retorno de Erro: Código 933 Duplicidade de Rubricas por Tipo
              AND D.IDERUBRICA_IDETABRUBR = 
              (SELECT  CASE
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idPenCiv'
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE = 5) THEN
                           'idPenMil'
                         WHEN (CB.COD_TIPO_BENEFICIO <> 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idAposen'
                         WHEN (CB.COD_TIPO_BENEFICIO IN('REFEO','REFAP') AND CB.COD_ENTIDADE = 5) THEN
                           'idRefor'
                         ELSE
                           'idReserv'
                       END
                FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB
                WHERE CB.COD_INS = CC.COD_INS
                  AND CB.COD_BENEFICIO = CC.COD_BENEFICIO
                  AND CB.COD_ENTIDADE = CC.COD_ENTIDADE
                )
              --TT79026 - 4.3.1  Critério de Busca Coluna COD_RUBRICA
              AND (CC.cod_tipo_beneficio = 'M'
                 AND EXISTS
                      (
                          SELECT 1 FROM user_ipesp.tb_impresao_rub ir
                           WHERE A.cod_ins = ir.cod_ins AND
                                 A.Cod_Fcrubrica = ir.cod_rubrica AND
                                 ir.cod_entidade = cc.cod_entidade AND
                                 ir.flg_imprime IN ('S','A')

                      ) OR cc.cod_tipo_beneficio != 'M' );

    BEGIN

        FOR C_RUBRICA_N_1 IN C_RUBRICA_N(P_CAD_FOLHA, P_CAD_DET_FOLHA) LOOP

            V_CPL_RUBRICAS_N.ID_RUBRICA_N := ESOCIAL.ESOC_SEQ_ID_1207_RUBRICA_N.NEXTVAL;
            V_CPL_RUBRICAS_N.ID_UNIDADE_N := P_ID_UNIDADE_N;
            V_CPL_RUBRICAS_N.CODRUBR      := C_RUBRICA_N_1.CODRUBR;
            V_CPL_RUBRICAS_N.IDETABRUBR   := C_RUBRICA_N_1.IDETABRUBR;
            V_CPL_RUBRICAS_N.QTDRUBR      := C_RUBRICA_N_1.QTDRUBR;
            V_CPL_RUBRICAS_N.FATORRUBR    := C_RUBRICA_N_1.FATORRUBR;
            V_CPL_RUBRICAS_N.VRUNIT       := C_RUBRICA_N_1.VRUNIT;
            V_CPL_RUBRICAS_N.VRRUBR       := C_RUBRICA_N_1.VRRUBR;

          SP_INC_1207_RUBRICA_N(V_CPL_RUBRICAS_N);

        END LOOP;

    END SP_1207_RUBRICAS_N;

    PROCEDURE SP_1207_ORGAO_UNIDADE_N(P_CAD_FOLHA        IN     ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                                      P_CAD_DET_FOLHA    IN     ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE,
                                      P_ID_DEMONSTRATIVO IN     ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO.ID_DEMONSTRATIVO%TYPE,
                                      o_retorno          IN OUT VARCHAR2) IS    -- v1.02

        V_ORG_UNI_N ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_N%ROWTYPE;

    BEGIN

        V_ORG_UNI_N.ID_UNIDADE_N     := ESOC_SEQ_ID_1207_UNI_N.NEXTVAL;
        V_ORG_UNI_N.ID_DEMONSTRATIVO := P_ID_DEMONSTRATIVO;
        V_ORG_UNI_N.TPINSC           := 1; --CNPJ
        V_ORG_UNI_N.NRINSC           := GB_EMPREGADOR.NUM_CNPJ; --FC_GET_CNPJ_ENT(P_CAD_DET_FOLHA.COD_BENEFICIO);

        SP_INC_ORG_UNIDADE_N(V_ORG_UNI_N);

        --Gera Rubricas
        BEGIN

            SP_1207_RUBRICAS_N(P_CAD_FOLHA, P_CAD_DET_FOLHA, V_ORG_UNI_N.ID_UNIDADE_N);

        EXCEPTION
            WHEN OTHERS THEN
                -- ROLLBACK;    -- v1.02
                GB_REC_ERRO.COD_INS           := GB_COD_INS;
                GB_REC_ERRO.ID_CAD            := P_CAD_FOLHA.ID_CAD_FOLHA;
                GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_RUBRICAS_N';
                GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                GB_REC_ERRO.DESC_ERRO         := 'ERRO AO GERAR RUBRICAS NORMAL NO EVENTO 1207';
                GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                GB_REC_ERRO.DES_IDENTIFICADOR := P_CAD_FOLHA.COD_IDE_CLI || ' ' ||P_CAD_DET_FOLHA.COD_BENEFICIO;
                GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                -- v1.02 - início
                -- SP_GERA_ERRO_PROCESSO;
                GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
                if (o_retorno is not null) then
                    o_retorno := substr(o_retorno || chr(10), 1, 4000);
                end if;
                o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
                -- v1.02 - fim
        END;

    -- v1.02 - início
    EXCEPTION
        WHEN OTHERS THEN
            GB_REC_ERRO.COD_INS           := GB_COD_INS;
            GB_REC_ERRO.ID_CAD            := P_CAD_FOLHA.ID_CAD_FOLHA;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_ORGAO_UNIDADE_N';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO AO EXECUTAR SP_1207_ORGAO_UNIDADE_N';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := P_CAD_FOLHA.COD_IDE_CLI || ' ' ||P_CAD_DET_FOLHA.COD_BENEFICIO;
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            if (o_retorno is not null) then
                o_retorno := substr(o_retorno || chr(10), 1, 4000);
            end if;
            o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
    -- v1.02 - fim

    END SP_1207_ORGAO_UNIDADE_N;

  FUNCTION FC_EXISTE_PGTO_TIPO(P_CAD_FOLHA     IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                               P_SEQ_PAGAMENTO IN ESOCIAL.TSOC_CAD_DET_FOLHA.SEQ_PAGAMENTO%TYPE,
                               P_TIP_PROCESSO  IN ESOCIAL.TSOC_CAD_DET_FOLHA.TIP_PROCESSO%TYPE,
                               P_TIPO          IN CHAR,
                               P_COD_BENEFICIO IN USER_IPESP.TB_CONCESSAO_BENEFICIO.COD_BENEFICIO%TYPE) RETURN BOOLEAN IS
    V_EXISTE NUMBER;
  BEGIN
    SELECT 1
      INTO V_EXISTE
      FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO A,
           USER_IPESP.TB_RUBRICAS       B,
           ESOCIAL.TSOC_CAD_RUBRICA     C,
           ESOCIAL.TSOC_1010_RUBRICA    D,
                 USER_IPESP.TB_CONCESSAO_BENEFICIO CC
     WHERE A.COD_IDE_CLI = P_CAD_FOLHA.COD_IDE_CLI
       AND A.COD_INS = P_CAD_FOLHA.COD_INS
       AND A.COD_BENEFICIO = P_COD_BENEFICIO
       AND A.PER_PROCESSO = P_CAD_FOLHA.PER_PROCESSO
       AND A.SEQ_PAGAMENTO = P_SEQ_PAGAMENTO
       AND A.TIP_PROCESSO = P_TIP_PROCESSO
          --Normal
       AND (      (TO_CHAR(A.DAT_INI_REF, 'MM/YYYY') = TO_CHAR(A.PER_PROCESSO, 'MM/YYYY') AND P_TIPO = 'N')
            OR
           --Retroativo
                  (TO_CHAR(A.DAT_INI_REF, 'MM/YYYY') <> TO_CHAR(A.PER_PROCESSO, 'MM/YYYY') AND P_TIPO = 'R')
            )

       AND B.COD_RUBRICA = A.COD_FCRUBRICA
       --AND B.COD_RUBRICA = C.COD_RUBRICA
       --TT
       AND D.ID_ORIGEM           = 1
       --TT
       --TICKET 79609 - Esocial SPPREV S-1207: Retorno de Erro: Código 933 Duplicidade de Rubricas por Tipo
              AND D.IDERUBRICA_IDETABRUBR = 
              (SELECT  CASE
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idPenCiv'
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE = 5) THEN
                           'idPenMil'
                         WHEN (CB.COD_TIPO_BENEFICIO <> 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idAposen'
                         WHEN (CB.COD_TIPO_BENEFICIO IN('REFEO','REFAP') AND CB.COD_ENTIDADE = 5) THEN
                           'idRefor'
                         ELSE
                           'idReserv'
                       END
                FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB
                WHERE CB.COD_INS = CC.COD_INS
                  AND CB.COD_BENEFICIO = CC.COD_BENEFICIO
                  AND CB.COD_ENTIDADE = CC.COD_ENTIDADE
                )
       AND TO_CHAR(B.COD_RUBRICA)         = C.COD_RUBRICA
       AND B.TIP_EVENTO_ESPECIAL <> 'P'
       AND B.COD_ENTIDADE = CC.COD_ENTIDADE
       -- LJUNIOR EM 02/09/2022 -- COMENTADO POR ORIRNTACAO DO CARLOS -- NAO DEVE RESTRINGIR RUBRICAS DE IR
       --AND TRUNC(B.COD_RUBRICA / 100) NOT IN (70012, 70014, 70078, 70081)
       AND CC.COD_INS = 1
              AND CC.COD_BENEFICIO = A.COD_BENEFICIO
              AND B.COD_ENTIDADE = CC.COD_ENTIDADE       
       AND D.ID_CAD_RUB = C.ID_CAD_RUB
       AND D.COD_INS = C.COD_INS
       AND D.FLG_VIGENCIA = 'A'
       AND ROWNUM = 1;

    RETURN TRUE;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN FALSE;

  END FC_EXISTE_PGTO_TIPO;

  --v_1.02 -  início
  FUNCTION FC_EXISTE_PGTO_TIPO_2(P_CAD_FOLHA     IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                                 P_SEQ_PAGAMENTO IN ESOCIAL.TSOC_CAD_DET_FOLHA.SEQ_PAGAMENTO%TYPE,
                                 P_TIP_PROCESSO  IN ESOCIAL.TSOC_CAD_DET_FOLHA.TIP_PROCESSO%TYPE,
                                 P_COD_BENEFICIO IN USER_IPESP.TB_CONCESSAO_BENEFICIO.COD_BENEFICIO%TYPE) RETURN VARCHAR2 IS
    v_retorno varchar2(10) := 'X';
  BEGIN
    FOR d in (SELECT distinct DAT_INI_REF
              FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO A,
                   USER_IPESP.TB_RUBRICAS       B,
                   ESOCIAL.TSOC_CAD_RUBRICA     C,
                   ESOCIAL.TSOC_1010_RUBRICA    D,
                 USER_IPESP.TB_CONCESSAO_BENEFICIO CC
             WHERE A.COD_INS        = P_CAD_FOLHA.COD_INS
               AND A.PER_PROCESSO   = P_CAD_FOLHA.PER_PROCESSO
               AND A.SEQ_PAGAMENTO  = P_SEQ_PAGAMENTO
               AND A.TIP_PROCESSO   = P_TIP_PROCESSO
               AND A.COD_BENEFICIO  = P_COD_BENEFICIO
               AND A.COD_IDE_CLI    = P_CAD_FOLHA.COD_IDE_CLI
               AND B.COD_INS     = A.COD_INS
               AND B.COD_RUBRICA = A.COD_FCRUBRICA
               AND D.ID_ORIGEM           = 1
               --TICKET 79609 - Esocial SPPREV S-1207: Retorno de Erro: Código 933 Duplicidade de Rubricas por Tipo
               AND D.IDERUBRICA_IDETABRUBR = 
              (SELECT  CASE
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idPenCiv'
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE = 5) THEN
                           'idPenMil'
                         WHEN (CB.COD_TIPO_BENEFICIO <> 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idAposen'
                         WHEN (CB.COD_TIPO_BENEFICIO IN('REFEO','REFAP') AND CB.COD_ENTIDADE = 5) THEN
                           'idRefor'
                         ELSE
                           'idReserv'
                       END
                FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB
                WHERE CB.COD_INS = CC.COD_INS
                  AND CB.COD_BENEFICIO = CC.COD_BENEFICIO
                  AND CB.COD_ENTIDADE = CC.COD_ENTIDADE
                )
               AND TO_CHAR(B.COD_RUBRICA)         = C.COD_RUBRICA
               AND B.TIP_EVENTO_ESPECIAL <> 'P'
               AND B.COD_ENTIDADE = CC.COD_ENTIDADE
               -- LJUNIOR EM 02/09/2022 -- COMENTADO POR ORIRNTACAO DO CARLOS -- NAO DEVE RESTRINGIR RUBRICAS DE IR
               -- AND TRUNC(B.COD_RUBRICA / 100) NOT IN (70012, 70014, 70078, 70081)
               
               AND CC.COD_INS = 1
              AND CC.COD_BENEFICIO = A.COD_BENEFICIO
              AND B.COD_ENTIDADE = CC.COD_ENTIDADE          
               AND D.ID_CAD_RUB = C.ID_CAD_RUB
               AND D.COD_INS = C.COD_INS
               AND D.FLG_VIGENCIA = 'A') loop

        if (instr(v_retorno, 'N') = 0) then
            --TT82342
            if (trunc(d.dat_ini_ref, 'MM') = trunc(P_CAD_FOLHA.PER_COMPETENCIA, 'MM')) then
                v_retorno := v_retorno || 'N';
            end if;
        end if;
        if (instr(v_retorno, 'R') = 0) then
            --TT82342
            if (trunc(d.dat_ini_ref, 'MM') != trunc(p_cad_folha.Per_Competencia, 'MM')) then
                v_retorno := v_retorno || 'R';
            end if;
        end if;
        if ((instr(v_retorno, 'N') != 0) and (instr(v_retorno, 'R') != 0)) then
            exit;
        end if;
    end loop;

    RETURN v_retorno;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN v_retorno;

  END FC_EXISTE_PGTO_TIPO_2;
  --v_1.02 -  fim

    FUNCTION FC_GET_PER_REF(P_COD_IDE_CLI   IN USER_IPESP.TB_HDET_CALCULADO.COD_IDE_CLI%TYPE,
                            P_COD_BENEFICIO IN USER_IPESP.TB_CONCESSAO_BENEFICIO.COD_BENEFICIO%TYPE,
                            P_PER_PROCESSO  IN USER_IPESP.TB_HDET_CALCULADO.PER_PROCESSO%TYPE,
                            P_SEQ_PAGAMENTO IN USER_IPESP.TB_HDET_CALCULADO.SEQ_PAGAMENTO%TYPE,
                            P_TIP_PROCESSO  IN USER_IPESP.TB_HDET_CALCULADO.TIP_PROCESSO%TYPE
                            )
            RETURN ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO.PERREF%TYPE IS

        V_PER_REF ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO.PERREF%TYPE;

    BEGIN

        SELECT TO_CHAR(HD.DAT_INI_REF,'YYYY-MM')
          INTO V_PER_REF
          FROM USER_IPESP.TB_ESOCIAL_HFOLHA HF, USER_IPESP.TB_ESOCIAL_HDET_CALCULADO HD
         WHERE HD.COD_INS = 1
           AND HD.COD_IDE_CLI = P_COD_IDE_CLI
           AND HD.COD_BENEFICIO = P_COD_BENEFICIO
           AND HD.PER_PROCESSO = P_PER_PROCESSO
           AND HD.SEQ_PAGAMENTO = P_SEQ_PAGAMENTO
           AND HD.TIP_PROCESSO = P_TIP_PROCESSO
           AND HD.DAT_INI_REF = HD.PER_PROCESSO
           AND HF.COD_INS = HD.COD_INS
           AND HF.COD_IDE_CLI = HD.COD_IDE_CLI
           AND HF.COD_BENEFICIO = HD.COD_BENEFICIO
           AND HF.PER_PROCESSO = HD.PER_PROCESSO
           AND HF.SEQ_PAGAMENTO = HD.SEQ_PAGAMENTO
           AND HF.TIP_PROCESSO = HD.TIP_PROCESSO
           AND (HD.DES_COMPLEMENTO LIKE 'Ret.%' or HD.DES_COMPLEMENTO = 'Parc.Deb');
          
           RETURN V_PER_REF;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN NULL;
    END FC_GET_PER_REF ;


    PROCEDURE SP_INC_1207_PROC_RETRO(P_1207_PROC_RETRAOTIVO ESOCIAL.TSOC_CPL_1207_PROC_RETROATIVO%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1207_PROC_RETROATIVO
            (ID_PROC_RETROATIVO,
             ID_DEMONSTRATIVO,
             DTACCONV,
             TPACCONV,
             COMPACCONV,
             DTEFACCONV,
             DSC,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_1207_PROC_RETRAOTIVO.ID_PROC_RETROATIVO,
             P_1207_PROC_RETRAOTIVO.ID_DEMONSTRATIVO,
             P_1207_PROC_RETRAOTIVO.DTACCONV,
             P_1207_PROC_RETRAOTIVO.TPACCONV,
             P_1207_PROC_RETRAOTIVO.COMPACCONV,
             P_1207_PROC_RETRAOTIVO.DTEFACCONV,
             P_1207_PROC_RETRAOTIVO.DSC,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_1207_PROC_RETRO');

        -- COMMIT;  -- v1.02

    END SP_INC_1207_PROC_RETRO;



    PROCEDURE SP_INC_1207_RETROATIVO(P_1207_RETRO IN ESOCIAL.TSOC_CPL_1207_RETROATIVO%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1207_RETROATIVO
            (ID_RETROATIVO,
             ID_PROC_RETROATIVO,
             PERREF,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_1207_RETRO.ID_RETROATIVO,
             P_1207_RETRO.ID_PROC_RETROATIVO,
             P_1207_RETRO.PERREF,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_1207_RETROATIVO');

        -- COMMIT;  -- v1.02

    END SP_INC_1207_RETROATIVO;


    PROCEDURE SP_INC_ORG_UNIDADE_R(P_ORG_UNI_R IN ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_R%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_R
            (ID_ORG_UNIDADE_R,
             ID_RETROATIVO,
             TPINSC,
             NRINSC,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_ORG_UNI_R.ID_ORG_UNIDADE_R,
             P_ORG_UNI_R.ID_RETROATIVO,
             P_ORG_UNI_R.TPINSC,
             P_ORG_UNI_R.NRINSC,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_ORG_UNIDADE_R');

        -- COMMIT;  -- v1.02

    END SP_INC_ORG_UNIDADE_R;


    PROCEDURE SP_INC_1207_RUBRICA_R(P_RUBRICA_R IN ESOCIAL.TSOC_CPL_1207_RUBRICA_R%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1207_RUBRICA_R
            (ID_RUBRICA_R,
             ID_ORG_UNIDADE_R,
             CODRUBR,
             IDETABRUBR,
             QTDRUBR,
             FATORRUBR,
             VRUNIT,
             VRRUBR,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_RUBRICA_R.ID_RUBRICA_R,
             P_RUBRICA_R.ID_ORG_UNIDADE_R,
             P_RUBRICA_R.CODRUBR,
             P_RUBRICA_R.IDETABRUBR,
             P_RUBRICA_R.QTDRUBR,
             P_RUBRICA_R.FATORRUBR,
             P_RUBRICA_R.VRUNIT,
             P_RUBRICA_R.VRRUBR,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_1207_RUBRICA_R');

        -- COMMIT;  -- v1.02

    END SP_INC_1207_RUBRICA_R;


    PROCEDURE SP_1207_RETROATIVO(P_CAD_FOLHA          IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                                 P_CAD_DET_FOLHA      IN ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE,
                                 P_ID_PROC_RETROATIVO IN ESOCIAL.TSOC_CPL_1207_PROC_RETROATIVO.ID_PROC_RETROATIVO%TYPE) IS

        CURSOR C_PER_RETRO(P_CAD_FOLHA     IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                           P_CAD_DET_FOLHA IN ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE) IS
            SELECT DISTINCT TRUNC(A.DAT_INI_REF,'MM') AS PERREF
            FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO A,
                 USER_IPESP.TB_RUBRICAS       B,
                 ESOCIAL.TSOC_CAD_RUBRICA     C,
                 ESOCIAL.TSOC_1010_RUBRICA    D,
                 USER_IPESP.TB_CONCESSAO_BENEFICIO CC
            WHERE A.COD_INS       = P_CAD_FOLHA.COD_INS
              AND A.PER_PROCESSO  = P_CAD_FOLHA.PER_PROCESSO
              AND A.SEQ_PAGAMENTO = P_CAD_DET_FOLHA.SEQ_PAGAMENTO
              AND A.TIP_PROCESSO  = P_CAD_DET_FOLHA.TIP_PROCESSO
              AND A.COD_BENEFICIO = P_CAD_DET_FOLHA.COD_BENEFICIO
              AND A.COD_IDE_CLI   = P_CAD_FOLHA.COD_IDE_CLI
              --Retroativo
              --TT82342
              AND TO_CHAR(A.DAT_INI_REF, 'MM/YYYY') <> TO_CHAR(A.PER_COMPETENCIA, 'MM/YYYY')
              AND B.COD_RUBRICA   = A.COD_FCRUBRICA
              AND D.ID_ORIGEM           = 1
              --TICKET 79609 - Esocial SPPREV S-1207: Retorno de Erro: Código 933 Duplicidade de Rubricas por Tipo
              AND D.IDERUBRICA_IDETABRUBR = 
              (SELECT  CASE
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idPenCiv'
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE = 5) THEN
                           'idPenMil'
                         WHEN (CB.COD_TIPO_BENEFICIO <> 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idAposen'
                         WHEN (CB.COD_TIPO_BENEFICIO IN('REFEO','REFAP') AND CB.COD_ENTIDADE = 5) THEN
                           'idRefor'
                         ELSE
                           'idReserv'
                       END
                FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB
                WHERE CB.COD_INS = CC.COD_INS
                  AND CB.COD_BENEFICIO = CC.COD_BENEFICIO
                  AND CB.COD_ENTIDADE = CC.COD_ENTIDADE
                )
              AND TO_CHAR(B.COD_RUBRICA)         = C.COD_RUBRICA
              AND B.TIP_EVENTO_ESPECIAL <> 'P'
              -- LJUNIOR EM 02/09/2022 -- COMENTADO POR ORIRNTACAO DO CARLOS -- NAO DEVE RESTRINGIR RUBRICAS DE IR
              -- AND TRUNC(B.COD_RUBRICA / 100) NOT IN (70012, 70014, 70078, 70081)
              AND CC.COD_INS = 1
              AND CC.COD_BENEFICIO = A.COD_BENEFICIO
              AND B.COD_ENTIDADE = CC.COD_ENTIDADE                         
              AND D.ID_CAD_RUB    = C.ID_CAD_RUB
              AND D.COD_INS       = C.COD_INS
              AND D.FLG_VIGENCIA  = 'A';

        CURSOR C_RUBRICA_R(P_CAD_FOLHA     IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                           P_CAD_DET_FOLHA IN ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE,
                           P_PER_REF       IN USER_IPESP.TB_ESOCIAL_HDET_CALCULADO.DAT_INI_REF%TYPE) IS
            SELECT D.IDERUBRICA_CODRUBR                         AS CODRUBR,
                   D.IDERUBRICA_IDETABRUBR                      AS IDETABRUBR,
                   /*(SELECT DECODE(E.VAL_INIDADE,0,NULL,E.VAL_INIDADE)
                    FROM USER_IPESP.TB_COMPOSICAO_BEN E
                    WHERE E.COD_BENEFICIO  = A.COD_BENEFICIO
                      AND E.COD_FCRUBRICA  = A.COD_FCRUBRICA
                      AND B.TIP_COMPOSICAO = 'B'
                      AND ROWNUM           = 1)                 AS QTDRUBR,*/
                      --TT82488 - 17/10/2022 - DALVES
                      CASE WHEN B.TIP_COMPOSICAO = 'B' THEN DECODE(TRUNC(A.VAL_INIDADE, 2),0,NULL,TRUNC(A.VAL_INIDADE, 2)) ELSE NULL END AS QTDRUBR,
                 /*  (SELECT DECODE(E.VAL_PORC,0,NULL,E.VAL_INIDADE)
                    FROM USER_IPESP.TB_COMPOSICAO_BEN E
                    WHERE E.COD_BENEFICIO  = A.COD_BENEFICIO
                      AND E.COD_FCRUBRICA  = A.COD_FCRUBRICA
                      AND B.TIP_COMPOSICAO = 'B'
                      AND ROWNUM           = 1)                 AS FATORRUBR,*/
                      --TT82488 - 17/10/2022 - DALVES
                      CASE WHEN B.TIP_COMPOSICAO = 'B' AND LENGTH(TRUNC(A.VAL_PORC)) <= 5 
                        THEN DECODE(TRUNC(A.VAL_PORC, 2),0,NULL,TRUNC(A.VAL_PORC, 2)) 
                        ELSE NULL 
                    END AS FATORRUBR,
                   A.VAL_RUBRICA                                AS VRUNIT,
                   A.VAL_RUBRICA                                AS VRRUBR,
                   A.DAT_INI_REF                                AS PERREF
            FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO A,
                 USER_IPESP.TB_RUBRICAS       B,
                 ESOCIAL.TSOC_CAD_RUBRICA     C,
                 ESOCIAL.TSOC_1010_RUBRICA    D,
                 USER_IPESP.TB_CONCESSAO_BENEFICIO CC
            WHERE A.COD_INS       = P_CAD_FOLHA.COD_INS
              AND A.PER_PROCESSO  = P_CAD_FOLHA.PER_PROCESSO
              AND A.SEQ_PAGAMENTO = P_CAD_DET_FOLHA.SEQ_PAGAMENTO
              AND A.TIP_PROCESSO  = P_CAD_DET_FOLHA.TIP_PROCESSO
              AND A.COD_BENEFICIO = P_CAD_DET_FOLHA.COD_BENEFICIO
              AND A.COD_IDE_CLI   = P_CAD_FOLHA.COD_IDE_CLI
              --Retroativo
              --TT82342
              AND TO_CHAR(A.DAT_INI_REF, 'MM/YYYY') <> TO_CHAR(A.PER_COMPETENCIA, 'MM/YYYY')
              AND B.COD_RUBRICA   = A.COD_FCRUBRICA
              AND D.ID_ORIGEM           = 1
              --TICKET 79609 - Esocial SPPREV S-1207: Retorno de Erro: Código 933 Duplicidade de Rubricas por Tipo
              AND D.IDERUBRICA_IDETABRUBR = 
              (SELECT  CASE
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idPenCiv'
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE = 5) THEN
                           'idPenMil'
                         WHEN (CB.COD_TIPO_BENEFICIO <> 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idAposen'
                         WHEN (CB.COD_TIPO_BENEFICIO IN('REFEO','REFAP') AND CB.COD_ENTIDADE = 5) THEN
                           'idRefor'
                         ELSE
                           'idReserv'
                       END
                FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB
                WHERE CB.COD_INS = CC.COD_INS
                  AND CB.COD_BENEFICIO = CC.COD_BENEFICIO
                  AND CB.COD_ENTIDADE = CC.COD_ENTIDADE
                )
              AND TO_CHAR(B.COD_RUBRICA)         = C.COD_RUBRICA
              AND B.TIP_EVENTO_ESPECIAL <> 'P'
              -- LJUNIOR EM 02/09/2022 -- COMENTADO POR ORIRNTACAO DO CARLOS -- NAO DEVE RESTRINGIR RUBRICAS DE IR
              --AND TRUNC(B.COD_RUBRICA / 100) NOT IN (70012, 70014, 70078, 70081)
              AND CC.COD_INS = 1
              AND CC.COD_BENEFICIO = A.COD_BENEFICIO
              AND B.COD_ENTIDADE = CC.COD_ENTIDADE                        
              AND D.ID_CAD_RUB    = C.ID_CAD_RUB
              AND D.COD_INS       = C.COD_INS
              AND D.FLG_VIGENCIA  = 'A'
              --TT82487
              AND TRUNC(A.DAT_INI_REF,'MM')   = P_PER_REF;

        V_1207_RETRO ESOCIAL.TSOC_CPL_1207_RETROATIVO%ROWTYPE;
        V_1207_ORG_UNI_R ESOCIAL.TSOC_CPL_1207_ORGAO_UNIDADE_R%ROWTYPE;
        V_1207_RUBRICA_R ESOCIAL.TSOC_CPL_1207_RUBRICA_R%ROWTYPE;

    BEGIN

        FOR C_RETRO IN C_PER_RETRO(P_CAD_FOLHA, P_CAD_DET_FOLHA) LOOP
            --RETROATIVO
            V_1207_RETRO.ID_RETROATIVO := ESOCIAL.ESOC_SEQ_ID_1207_RETROATIVO.NEXTVAL;
            V_1207_RETRO.ID_PROC_RETROATIVO := P_ID_PROC_RETROATIVO;
            V_1207_RETRO.PERREF := TO_CHAR(C_RETRO.PERREF,'YYYY-MM');

            SP_INC_1207_RETROATIVO(V_1207_RETRO);

            --ORGAO UNIDADE RETRO
            V_1207_ORG_UNI_R.ID_ORG_UNIDADE_R  := ESOC_SEQ_ID_1207_UNI_R.NEXTVAL;
            V_1207_ORG_UNI_R.ID_RETROATIVO   := V_1207_RETRO.ID_RETROATIVO;
            V_1207_ORG_UNI_R.TPINSC            := 1; --CNPJ
            V_1207_ORG_UNI_R.NRINSC            := GB_EMPREGADOR.NUM_CNPJ; --FC_GET_CNPJ_ENT(P_CAD_DET_FOLHA.COD_BENEFICIO);
            
            
            SP_INC_ORG_UNIDADE_R(V_1207_ORG_UNI_R);
            
            FOR C_RUB IN C_RUBRICA_R(P_CAD_FOLHA, P_CAD_DET_FOLHA, C_RETRO.PERREF ) LOOP

                V_1207_RUBRICA_R.ID_RUBRICA_R     := ESOCIAL.ESOC_SEQ_ID_1207_RUBRICA_R.NEXTVAL;
                V_1207_RUBRICA_R.ID_ORG_UNIDADE_R := V_1207_ORG_UNI_R.ID_ORG_UNIDADE_R;
                V_1207_RUBRICA_R.CODRUBR          := C_RUB.CODRUBR;
                V_1207_RUBRICA_R.IDETABRUBR       := C_RUB.IDETABRUBR;--TT81955
                V_1207_RUBRICA_R.QTDRUBR          := C_RUB.QTDRUBR;
                V_1207_RUBRICA_R.FATORRUBR        := C_RUB.FATORRUBR;
                V_1207_RUBRICA_R.VRUNIT           := C_RUB.VRUNIT;
                V_1207_RUBRICA_R.VRRUBR           := C_RUB.VRRUBR;

                SP_INC_1207_RUBRICA_R(V_1207_RUBRICA_R);

            END LOOP;
            
        END LOOP;

END SP_1207_RETROATIVO;


    PROCEDURE SP_1207_PROC_RETROATIVO(P_CAD_FOLHA        IN     ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                                      P_DET_FOLHA        IN     ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE,
                                      P_ID_DEMONSTRATIVO IN     ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO.ID_DEMONSTRATIVO%TYPE,
                                      o_retorno          IN OUT VARCHAR2) IS    -- v1.02

        V_1207_PROC_RETROATIVO ESOCIAL.TSOC_CPL_1207_PROC_RETROATIVO%ROWTYPE;

    BEGIN

        --1 Demonstrativo - 1 Processo Judicial por PER_PROCESSO,IDE_CLI, BENEFICIO
        BEGIN
            SELECT CR.DAT_INI_VIG,
                   TO_CHAR(CR.PER_PROCESSO, 'YYYY/MM'),
                   CR.NOM_CRITERIO
            INTO V_1207_PROC_RETROATIVO.DTACCONV,
                 V_1207_PROC_RETROATIVO.COMPACCONV,
                 V_1207_PROC_RETROATIVO.DSC
            FROM USER_IPESP.TB_CRITERIOS_RETROATIVOS_BEN CRB,
                 USER_IPESP.TB_CRITERIOS_RETROATIVOS     CR
            WHERE CRB.COD_INS       = P_CAD_FOLHA.COD_INS
              AND CRB.COD_IDE_CLI   = P_CAD_FOLHA.COD_IDE_CLI
              AND CRB.COD_BENEFICIO = P_DET_FOLHA.COD_BENEFICIO
              AND CRB.FLG_STATUS    = 'V'
              AND CR.COD_INS        = CRB.COD_INS
              AND CR.COD_CRITERIO   = CRB.COD_CRITERIO
              AND CR.PER_PROCESSO   = P_CAD_FOLHA.PER_PROCESSO
              AND ROWNUM            = 1;

            V_1207_PROC_RETROATIVO.ID_PROC_RETROATIVO := ESOCIAL.ESOC_SEQ_ID_1207_PROC_RETRO.NEXTVAL;
            V_1207_PROC_RETROATIVO.ID_DEMONSTRATIVO   := P_ID_DEMONSTRATIVO;
            V_1207_PROC_RETROATIVO.TPACCONV           := 'B'; --Se existir processo, enviar 'B'
            -- V_1207_PROC_RETROATIVO.DTACCONV           := V_1207_PROC_RETROATIVO.DTACCONV; -- (?!) v1.02

            SP_INC_1207_PROC_RETRO(V_1207_PROC_RETROATIVO);

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                V_1207_PROC_RETROATIVO.ID_PROC_RETROATIVO := ESOCIAL.ESOC_SEQ_ID_1207_PROC_RETRO.NEXTVAL;
                V_1207_PROC_RETROATIVO.ID_DEMONSTRATIVO   := P_ID_DEMONSTRATIVO;
                V_1207_PROC_RETROATIVO.TPACCONV           := 'G'; --Se existir processo, enviar 'B'
                V_1207_PROC_RETROATIVO.DSC                := 'Decisão Administrativa';

                SP_INC_1207_PROC_RETRO(V_1207_PROC_RETROATIVO);

        END;

        SP_1207_RETROATIVO(P_CAD_FOLHA,
                           P_DET_FOLHA,
                           V_1207_PROC_RETROATIVO.ID_PROC_RETROATIVO);

    -- v1.02 - início
    EXCEPTION
        WHEN OTHERS THEN
            GB_REC_ERRO.COD_INS           := GB_COD_INS;
            GB_REC_ERRO.ID_CAD            := P_CAD_FOLHA.ID_CAD_FOLHA;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_PROC_RETROATIVO';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO AO EXECUTAR SP_1207_PROC_RETROATIVO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := P_CAD_FOLHA.COD_IDE_CLI || ' ' ||P_DET_FOLHA.COD_BENEFICIO;
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            if (o_retorno is not null) then
                o_retorno := substr(o_retorno || chr(10), 1, 4000);
            end if;
            o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
    -- v1.02 - fim

    END SP_1207_PROC_RETROATIVO;


    PROCEDURE SP_1207_DEMONSTRATIVO(P_CAD_FOLHA IN      ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                                    P_ID_PK     IN      ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO.ID_PK%TYPE,
                                    O_RETORNO   IN OUT  VARCHAR2) IS    -- v1.02

        V_1207_DEM  TSOC_CPL_1207_DEMONSTRATIVO%ROWTYPE;
        V_DET_FOLHA ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE;
        -- LJUNIOR em 05/09/2022 - Ajuste. Estouro no conteudo da varíavel
        v_existe_pgto_tipo varchar2(3); -- v_1.02
        V_CAD_FOLHA ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE;

    BEGIN
      
        V_CAD_FOLHA := P_CAD_FOLHA;

        FOR C_DEM IN C_CAD_DET_FOLHA(P_CAD_FOLHA.COD_IDE_CLI) LOOP

            V_1207_DEM.ID_DEMONSTRATIVO := ESOC_SEQ_ID_1207_DEM.NEXTVAL;
            V_1207_DEM.ID_PK            := P_ID_PK;
            V_1207_DEM.IDEMDEV          := C_DEM.NRBENEFICIO ||
                                                TO_CHAR(C_DEM.PER_PROCESSO, 'MMYYYY') ||
                                                C_DEM.SEQ_PAGAMENTO ||
                                                C_DEM.TIP_PROCESSO;
            V_1207_DEM.NRBENEFICIO      := C_DEM.NRBENEFICIO;
            V_1207_DEM.SEQ_PAGAMENTO    := C_DEM.SEQ_PAGAMENTO;
            V_1207_DEM.PERREF           := null; /*FC_GET_PER_REF(P_CAD_FOLHA.COD_IDE_CLI,
                                                          C_DEM.COD_BENEFICIO,
                                                          P_CAD_FOLHA.PER_PROCESSO,
                                                          C_DEM.SEQ_PAGAMENTO,
                                                          C_DEM.TIP_PROCESSO
                                                          );*/

            SP_INC_1207_DEM(V_1207_DEM);

            V_DET_FOLHA.TIP_PROCESSO  := C_DEM.TIP_PROCESSO;
            V_DET_FOLHA.SEQ_PAGAMENTO := C_DEM.SEQ_PAGAMENTO;
            V_DET_FOLHA.COD_BENEFICIO := C_DEM.COD_BENEFICIO;
            V_CAD_FOLHA.PER_PROCESSO  := C_DEM.PER_PROCESSO;

            -- v1.02 - início
            v_existe_pgto_tipo := FC_EXISTE_PGTO_TIPO_2(V_CAD_FOLHA,
                                                        C_DEM.SEQ_PAGAMENTO,
                                                        C_DEM.TIP_PROCESSO,
                                                        C_DEM.COD_BENEFICIO);

            --Se existir pagamento normal executa cadastro nas tabelas de pagamento normal
            IF (instr(v_existe_pgto_tipo, 'N') > 0) THEN
            --IF FC_EXISTE_PGTO_TIPO(V_CAD_FOLHA,
            --                       C_DEM.SEQ_PAGAMENTO,
            --                       C_DEM.TIP_PROCESSO,
            --                       'N',
            --                       C_DEM.COD_BENEFICIO) THEN
            -- v1.02 - fim

                -----1207_ORGAO_UNIDADE_N----------------------------
                BEGIN
                    SP_1207_ORGAO_UNIDADE_N(V_CAD_FOLHA,
                                            V_DET_FOLHA,
                                            V_1207_DEM.ID_DEMONSTRATIVO,
                                            o_retorno);  -- v1.02
                EXCEPTION
                    WHEN OTHERS THEN
                        -- ROLLBACK;    -- v1.02
                        GB_REC_ERRO.COD_INS           := GB_COD_INS;
                        GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                        GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_ORGAO_UNIDADE_N';
                        GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                        GB_REC_ERRO.DESC_ERRO         := 'ERRO AO GERAR EVENTO DE FOLHA EM TSOC_CPL_1207_ORGAO_UNIDADE_N';
                        GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                        GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI || ' ' ||C_DEM.COD_BENEFICIO;
                        GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                        GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                        -- v1.02 - início
                        -- SP_GERA_ERRO_PROCESSO;
                        GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                        SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
                        if (o_retorno is not null) then
                            o_retorno := substr(o_retorno || chr(10), 1, 4000);
                        end if;
                        o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);

                        exit;
                        -- v1.02 - fim
                END;

                -- v1.02 - início
                if (o_retorno is not null) then
                    exit;
                end if;
                -- v1.02 - fim

            END IF;
            ----------------------------------------------------------------------
            -- v1.02 - início
            --Se existir pagamento retroativo executa cadastro nas tabelas de pagamento reatroativo
            IF (instr(v_existe_pgto_tipo, 'R') > 0) THEN
            --IF FC_EXISTE_PGTO_TIPO(V_CAD_FOLHA,
            --                       C_DEM.SEQ_PAGAMENTO,
            --                       C_DEM.TIP_PROCESSO,
            --                       'R',
            --                       C_DEM.COD_BENEFICIO) THEN
            -- v1.02 - fim

                BEGIN
                    -----1207_ORGAO_UNIDADE_N----------------------------
                    SP_1207_PROC_RETROATIVO(V_CAD_FOLHA,
                                            V_DET_FOLHA,
                                            V_1207_DEM.ID_DEMONSTRATIVO,
                                            o_retorno); -- v1.02
                EXCEPTION
                    WHEN OTHERS THEN
                        -- ROLLBACK;    -- v1.02
                        GB_REC_ERRO.COD_INS           := GB_COD_INS;
                        GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                        GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_PROC_RETROATIVO';
                        GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                        GB_REC_ERRO.DESC_ERRO         := 'ERRO AO GERAR PAGAMENTOS RETROATIVOS';
                        GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                        GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI || ' ' ||C_DEM.COD_BENEFICIO;
                        GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                        GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                        -- v1.02 - início
                        -- SP_GERA_ERRO_PROCESSO;
                        GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                        SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
                        if (o_retorno is not null) then
                            o_retorno := substr(o_retorno || chr(10), 1, 4000);
                        end if;
                        o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);

                        exit;
                        -- v1.02 - fim
                END;

                -- v1.02 - início
                if (o_retorno is not null) then
                    exit;
                end if;
                -- v1.02 - fim

            END IF;

        END LOOP;

    -- v1.02 - início
    EXCEPTION
        WHEN OTHERS THEN
            GB_REC_ERRO.COD_INS           := GB_COD_INS;
            GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_DEMONSTRATIVO';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO AO PROCESSAR SP_1207_DEMONSTRATIVO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            if (o_retorno is not null) then
                o_retorno := substr(o_retorno || chr(10), 1, 4000);
            end if;
            o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
    -- v1.02 - fim
    END SP_1207_DEMONSTRATIVO;


    PROCEDURE SP_1207_BENEFICIO(P_CAD_FOLHA IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE,
                                o_retorno   in out varchar2) IS 

        V_1207_BENEFICIO ESOCIAL.TSOC_1207_BENEFICIO%ROWTYPE;

    BEGIN

        V_1207_BENEFICIO.ID_PK        := ESOC_SEQ_ID_1207.NEXTVAL;
        V_1207_BENEFICIO.ID_CAD_FOLHA := P_CAD_FOLHA.ID_CAD_FOLHA;
        V_1207_BENEFICIO.ID_ORIGEM    := GB_ID_ORIGEM;
        V_1207_BENEFICIO.ID_EVENTO    := GB_ID_EVENTO;
        V_1207_BENEFICIO.SEQ_EVENTO   := 1;
        V_1207_BENEFICIO.COD_INS      := GB_COD_INS;

        SP_RET_INFO_AMBIENTE;
        SP_RET_INSC_EMP;

        V_1207_BENEFICIO.ID := FC_GERA_ID_EVENTO;
        
        --TT84697
        V_1207_BENEFICIO.INDRETIF    := GB_IND_RETIF;

        --TT83733
        IF GB_ID_APURACAO = 2 THEN
          V_1207_BENEFICIO.INDAPURACAO := 2;
          V_1207_BENEFICIO.PERAPUR     := to_char(GB_PER_COMPETENCIA,'YYYY');
        ELSE
          V_1207_BENEFICIO.INDAPURACAO := 1;
          V_1207_BENEFICIO.PERAPUR     := to_char(GB_PER_COMPETENCIA,'YYYY-MM');
        END IF;

        --SP_RET_INFO_AMBIENTE;     -- v1.01
        V_1207_BENEFICIO.PROCEMI := GB_AMB.PROCEMI;
        V_1207_BENEFICIO.VERPROC := GB_AMB.VERPROC;
        V_1207_BENEFICIO.TPAMB   := GB_AMB.TPAMB;


        --SP_RET_INSC_EMP;          -- v1.01
        V_1207_BENEFICIO.TPINSC := GB_EMPREGADOR.TP_INSC;
        V_1207_BENEFICIO.NRINSC := GB_EMPREGADOR.NUM_CNPJ;

        --v1.02 - início
        --V_1207_BENEFICIO.CPFBENEF       := FC_CPF_BEN(P_CAD_FOLHA.COD_IDE_CLI);
        V_1207_BENEFICIO.CPFBENEF       := P_CAD_FOLHA.NUM_CPF;
        --v1.02 - fim
        V_1207_BENEFICIO.CTR_FLG_STATUS := 'AX';
        V_1207_BENEFICIO.FLG_VIGENCIA   := 'A';
        V_1207_BENEFICIO.ID_PERIODO_DET := GB_ID_PERIODO_DET;

        SP_INC_1207_BENEFICIO(V_1207_BENEFICIO);

        BEGIN
            SP_1207_DEMONSTRATIVO(P_CAD_FOLHA,
                                  V_1207_BENEFICIO.ID_PK,
                                  o_retorno);   -- v1.02
        EXCEPTION
            WHEN OTHERS THEN
                -- ROLLBACK;    -- v1.02
                GB_REC_ERRO.COD_INS           := GB_COD_INS;
                GB_REC_ERRO.ID_CAD            := P_CAD_FOLHA.ID_CAD_FOLHA;
                GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_BENEFICIO';
                GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                GB_REC_ERRO.DESC_ERRO         := 'ERRO AO GERAR EVENTO DE FOLHA EM TSOC_CPL_1207_DEMONSTRATIVO';
                GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                GB_REC_ERRO.DES_IDENTIFICADOR := P_CAD_FOLHA.COD_IDE_CLI;
                GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                -- v1.02 - início
                -- SP_GERA_ERRO_PROCESSO;
                GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
                if (o_retorno is not null) then
                    o_retorno := substr(o_retorno || chr(10), 1, 4000);
                end if;
                o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
                -- v1.02 - fim
        END;

    -- v1.02 - início
    EXCEPTION
        WHEN OTHERS THEN
            GB_REC_ERRO.COD_INS           := GB_COD_INS;
            GB_REC_ERRO.ID_CAD            := P_CAD_FOLHA.ID_CAD_FOLHA;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_BENEFICIO';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO AO GERAR EVENTO DE FOLHA EM TSOC_1207_BENEFICIO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := P_CAD_FOLHA.COD_IDE_CLI;
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            if (o_retorno is not null) then
                o_retorno := substr(o_retorno || chr(10), 1, 4000);
            end if;
            o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
    -- v1.02 - fim
    END SP_1207_BENEFICIO;

    /*
    PROCEDURE  SP_1207_DEMONSTRATIVO(P_CAD_FOLHA IN ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE)
    IS
    V_1207_DEM TSOC_CPL_1207_DEMONSTRATIVO%ROWTYPE;
    BEGIN
      FOR C_DEM IN C_CAD_DET_FOLHA(P_CAD_FOLHA.COD_IDE_CLI,P_CAD_FOLHA.COD_BENEFICIO)
             LOOP

              V_1207_DEM.ID_DEMONSTRATIVO := ESOC_SEQ_ID_1207_DEM.NEXTVAL;
              V_1207_DEM.ID_PK := GB_ID_PK;
              V_1207_DEM.IDEMDEV := P_CAD_FOLHA.COD_BENEFICIO||TO_CHAR(P_CAD_FOLHA.PER_PROCESSO,'MMYYYY')||C_DEM.SEQ_PAGAMENTO||C_DEM.TIP_PROCESSO;
              V_1207_DEM.NRBENEFICIO := P_CAD_FOLHA.COD_BENEFICIO;

            END LOOP;


      SP_INC_1207_DEM(V_1207_DEM);


    END SP_1207_DEMONSTRATIVO;*/

    --GERA CADASTRO DE FOLHA E EVENTO 1207 DE BENEFÍCIOS - ENTES PÚBLICOS
    PROCEDURE SP_CAD_FOLHA_1207(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS

        V_CAD_FOLHA     ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE;
        V_CAD_DET_FOLHA ESOCIAL.TSOC_CAD_DET_FOLHA%ROWTYPE;
        V_NRBENEFICIO   ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO.IDEMDEV%TYPE;
        EX_PARAM_PROC   EXCEPTION;
        v_retorno       varchar2(4000);

    BEGIN

        --Valida parametrização
        GB_ID_CTR_PROCESSO := P_ID_CTR_PROCESSO;
        BEGIN
            SP_CARREGA_IDS;
        EXCEPTION
            WHEN OTHERS THEN
            RAISE EX_PARAM_PROC;
        END;

        SP_SETA_PROCESSO('INICIO_PROCESSAMENTO');

        commit; -- v1.02

        --Obtém Per processo
        SP_SET_PER_PROCESSO;

        -- v1.01 - Início
        SP_RET_INFO_AMBIENTE;
        SP_RET_INSC_EMP;
        -- v1.01 - Fim

        --Carrega cursor CAD_FOLHA
        IF NOT C_CAD_FOLHA%ISOPEN THEN
            OPEN C_CAD_FOLHA(GB_FAIXA_INI_CPF,GB_FAIXA_FIM_CPF);
        END IF;
        LOOP
            BEGIN

                FETCH C_CAD_FOLHA
                INTO V_CAD_FOLHA.COD_INS,
                     V_CAD_FOLHA.COD_IDE_CLI,
                     V_CAD_FOLHA.PER_PROCESSO,
                     --TT82342 - Folha de Recadastramento e Suplementar
                     V_CAD_FOLHA.PER_COMPETENCIA,
                     V_CAD_FOLHA.NUM_CPF;    -- v1.02

                EXIT WHEN C_CAD_FOLHA%NOTFOUND;

                --PK
                V_CAD_FOLHA.ID_CAD_FOLHA := ESOC_SEQ_ID_CAD_FOLHA.NEXTVAL;

                ---------------------INSERE NO CADASTRO DE FOLHA------------------
                BEGIN
                    SP_INC_CAD_FOLHA(V_CAD_FOLHA);
                    -- COMMIT; -- v1.02

                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;    -- v1.02
                        GB_REC_ERRO.COD_INS           := GB_COD_INS;
                        GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                        GB_REC_ERRO.NOM_PROCESSO      := 'SP_INC_CAD_FOLHA';
                        GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                        GB_REC_ERRO.DESC_ERRO         := 'ERRO NA INCLUSÃO DE CADASTRO DE FOLHA';
                        GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                        GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
                        GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
                        GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                        -- v1.02 - início
                        -- SP_GERA_ERRO_PROCESSO;
                        GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                        SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);

                        continue;
                        -- v1.02 - fim
                END;

                ---------------------------Obtém detalhe de folha---------------------------------------------

                IF NOT C_CAD_DET_FOLHA%ISOPEN THEN
                    OPEN C_CAD_DET_FOLHA(V_CAD_FOLHA.COD_IDE_CLI);
                END IF;

                LOOP

                    FETCH C_CAD_DET_FOLHA
                    INTO V_CAD_DET_FOLHA.TIP_PROCESSO,
                         V_CAD_DET_FOLHA.SEQ_PAGAMENTO,
                         V_CAD_DET_FOLHA.DT_FECHAMENTO,
                         V_CAD_DET_FOLHA.NUM_GRP,
                         V_CAD_DET_FOLHA.VAL_SAL_BASE,
                         V_CAD_DET_FOLHA.TOT_CRED,
                         V_CAD_DET_FOLHA.TOT_DEB,
                         V_CAD_DET_FOLHA.VAL_LIQUIDO,
                         V_CAD_DET_FOLHA.COD_ENTIDADE,
                         V_NRBENEFICIO,
                         V_CAD_DET_FOLHA.COD_BENEFICIO,
                         V_CAD_DET_FOLHA.PER_PROCESSO;

                    EXIT WHEN C_CAD_DET_FOLHA%NOTFOUND;

                    V_CAD_DET_FOLHA.ID_DET_CAD   := ESOC_SEQ_ID_DET_CAD_FOLHA.NEXTVAL;  --PK
                    V_CAD_DET_FOLHA.ID_CAD_FOLHA := V_CAD_FOLHA.ID_CAD_FOLHA;           --FK

                    --V_CAD_FOLHA.ID_CAD_FOLHA := ESOC_SEQ_ID_CAD_FOLHA.NEXTVAL;
                    ---------------------INSERE DETALHE FOLHA------------------
                    BEGIN
                        SP_INC_CAD_DET_FOLHA(V_CAD_DET_FOLHA);
                        -- COMMIT; -- v1.02

                    EXCEPTION
                        WHEN OTHERS THEN
                            ROLLBACK;    -- v1.02
                            GB_REC_ERRO.COD_INS           := GB_COD_INS;
                            GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                            GB_REC_ERRO.NOM_PROCESSO      := 'SP_INC_CAD_FOLHA';
                            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                            GB_REC_ERRO.DESC_ERRO         := 'ERRO NA INCLUSÃO DE CADASTRO DE DETALHE DE FOLHA';
                            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                            GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI || ' ' || V_CAD_DET_FOLHA.COD_BENEFICIO;
                            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
                            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                            -- v1.02 - início
                            -- SP_GERA_ERRO_PROCESSO;
                            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);

                            continue;
                            -- v1.02 - fim
                    END;

                END LOOP;

                CLOSE C_CAD_DET_FOLHA;

                ------------------------------GERA EVENTO-----------------------------
                -----1207_BENEFICIO----------------------------
                -- v1.01 - início
                -- GB_DAT_EVT_ANT := GB_DAT_EVT_ATU;
                -- GB_DAT_EVT_ATU := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MMSS');
                -- v1.01 - fim
                GB_IND_RETIF := 1;
               
                BEGIN
                    v_retorno := null;    
                    SP_1207_BENEFICIO(V_CAD_FOLHA,
                                      v_retorno);       -- v1.02
                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;    -- v1.02
                        GB_REC_ERRO.COD_INS           := GB_COD_INS;
                        GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                        GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_BENEFICIO';
                        GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                        GB_REC_ERRO.DESC_ERRO         := 'ERRO AO GERAR EVENTO DE FOLHA EM TSOC_1207_BENEFICIO';
                        GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                        GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
                        GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                        GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                        -- v1.02 - início
                        -- SP_GERA_ERRO_PROCESSO;
                        GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                        SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);

                        continue;
                        -- v1.02 - fim
                END;

                -- v1.02 - início
                -- SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');     -- Atualiza Quantidade
                if (v_retorno is null) then
                    SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');     -- Atualiza Quantidade
                    commit;
                else
                    rollback;
                end if;
                -- v1.02 - fim

            --Exceções
            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK;    -- v1.02
                    GB_REC_ERRO.COD_INS           := GB_COD_INS;
                    GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                    GB_REC_ERRO.NOM_PROCESSO      := 'SP_INC_CAD_FOLHA';
                    GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                    GB_REC_ERRO.DESC_ERRO         := 'ERRO NA INCLUSÃO DE CADASTRO DE DETALHE DE FOLHA';
                    GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                    GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
                    GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
                    GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                    -- v1.02 - início
                    -- SP_GERA_ERRO_PROCESSO;
                    GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                    SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
                    -- v1.02 - fim
            END;
        END LOOP;

        --Finalizar processo

        SP_SETA_PROCESSO('FIM_PROCESSAMENTO');

        commit;     -- v1.02

    EXCEPTION
        WHEN EX_PARAM_PROC THEN
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_CAD_FOLHA';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO NA PARAMETRIZAÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

        WHEN OTHERS THEN
            ROLLBACK;
            GB_REC_ERRO.COD_INS           := GB_COD_INS;
            GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_INC_CAD_FOLHA';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO DE EXECUÇÃO NO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

    END SP_CAD_FOLHA_1207;
    --TICKET 84697 - DALVES - 18/01/2023
    --GERA RETIFICAÇÃO DE FOLHA E EVENTO 1207 DE BENEFÍCIOS - ENTES PÚBLICOS
    PROCEDURE SP_RET_FOLHA_1207(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS

        V_CAD_FOLHA     ESOCIAL.TSOC_CAD_FOLHA%ROWTYPE;

        EX_PARAM_PROC   EXCEPTION;
        v_retorno       varchar2(4000);

    BEGIN

        --Valida parametrização
        GB_ID_CTR_PROCESSO := P_ID_CTR_PROCESSO;
        BEGIN
            SP_CARREGA_IDS;
        EXCEPTION
            WHEN OTHERS THEN
            RAISE EX_PARAM_PROC;
        END;
        --TESTE
        --SP_SETA_PROCESSO('INICIO_PROCESSAMENTO');

        --commit; -- v1.02

        --Obtém Per processo
        SP_SET_PER_PROCESSO;

        -- v1.01 - Início
        SP_RET_INFO_AMBIENTE;
        SP_RET_INSC_EMP;
        -- v1.01 - Fim

        --Carrega cursor de retificações RET_FOLHA
        IF NOT C_RET_FOLHA%ISOPEN THEN
            OPEN C_RET_FOLHA(GB_FAIXA_INI_CPF,GB_FAIXA_FIM_CPF);
        END IF;
        LOOP
            BEGIN

                FETCH C_RET_FOLHA
                INTO V_CAD_FOLHA.COD_INS,
                     V_CAD_FOLHA.COD_IDE_CLI,
                     V_CAD_FOLHA.PER_PROCESSO,
                     V_CAD_FOLHA.PER_COMPETENCIA,
                     V_CAD_FOLHA.NUM_CPF,
                     V_CAD_FOLHA.ID_CAD_FOLHA,
                     V_CAD_FOLHA.ID_APURACAO,
                     GB_NR_RECIBO;
                     

                EXIT WHEN C_RET_FOLHA%NOTFOUND;

                --GUARDAR O REGISTRO ORIGINAL NA TABELA DE HISTÓRICO
                SP_INC_H1207_BENEFICIO(V_CAD_FOLHA.ID_CAD_FOLHA);
                --EXCLUIR O REGISTRO ORIGINAL DA TSOC E CPLS
                SP_DEL_1207_BENEFICIO(V_CAD_FOLHA.ID_CAD_FOLHA);

                ------------------------------GERA EVENTO-----------------------------
                --GB_NR_RECIBO := 'WS_NUM_RECIBO';
                GB_IND_RETIF := 2;
                
                BEGIN
                    v_retorno := null;
                    SP_1207_BENEFICIO(V_CAD_FOLHA,
                                      v_retorno); 
                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        GB_REC_ERRO.COD_INS           := GB_COD_INS;
                        GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                        GB_REC_ERRO.NOM_PROCESSO      := 'SP_1207_BENEFICIO';
                        GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                        GB_REC_ERRO.DESC_ERRO         := 'ERRO AO GERAR EVENTO DE FOLHA EM TSOC_1207_BENEFICIO';
                        GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                        GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
                        GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                        GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                        -- v1.02 - início
                        -- SP_GERA_ERRO_PROCESSO;
                        GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                        SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);

                        continue;
                        -- v1.02 - fim
                END;

                -- v1.02 - início
                -- SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');     -- Atualiza Quantidade
                if (v_retorno is null) then
                    SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');     -- Atualiza Quantidade
                    commit;
                else
                    rollback;
                end if;
                -- v1.02 - fim

            --Exceções
            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK;    -- v1.02
                    GB_REC_ERRO.COD_INS           := GB_COD_INS;
                    GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
                    GB_REC_ERRO.NOM_PROCESSO      := 'SP_INC_CAD_FOLHA';
                    GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                    GB_REC_ERRO.DESC_ERRO         := 'ERRO NA INCLUSÃO DE CADASTRO DE DETALHE DE FOLHA';
                    GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                    GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
                    GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
                    GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                    -- v1.02 - início
                    -- SP_GERA_ERRO_PROCESSO;
                    GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                    SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
                    -- v1.02 - fim
            END;
        END LOOP;

        --Finalizar processo

        SP_SETA_PROCESSO('FIM_PROCESSAMENTO');

        commit;     -- v1.02

    EXCEPTION
        WHEN EX_PARAM_PROC THEN
            ROLLBACK;
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_CAD_FOLHA';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO NA PARAMETRIZAÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

        WHEN OTHERS THEN
            ROLLBACK;
            GB_REC_ERRO.COD_INS           := GB_COD_INS;
            GB_REC_ERRO.ID_CAD            := V_CAD_FOLHA.ID_CAD_FOLHA;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_INC_CAD_FOLHA';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO DE EXECUÇÃO NO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := V_CAD_FOLHA.COD_IDE_CLI;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

    END SP_RET_FOLHA_1207;


    FUNCTION FC_GET_IDEDMDEV(P_ID_CAD_FOLHA  NUMBER,
                             P_COD_BENEFICIO NUMBER,
                             P_SEQ_PAGAMENTO NUMBER)
            RETURN ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR.IDEDMDEV%TYPE IS

        V_IDEDMDEV ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR.IDEDMDEV%TYPE;

    BEGIN

        SELECT DM.IDEMDEV
        INTO V_IDEDMDEV
        FROM ESOCIAL.TSOC_CAD_DET_FOLHA          CF,
             ESOCIAL.TSOC_1207_BENEFICIO         TB,
             ESOCIAL.TSOC_CPL_1207_DEMONSTRATIVO DM
        WHERE TB.ID_CAD_FOLHA = CF.ID_CAD_FOLHA
          AND DM.ID_PK = TB.ID_PK
          --AND CF.COD_BENEFICIO = DM.NRBENEFICIO
          AND CF.ID_CAD_FOLHA = P_ID_CAD_FOLHA
          AND DM.NRBENEFICIO = TB.CPFBENEF||P_COD_BENEFICIO
          AND DM.SEQ_PAGAMENTO = P_SEQ_PAGAMENTO;

        RETURN V_IDEDMDEV;

    /*EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;*/

    END FC_GET_IDEDMDEV;

    FUNCTION FC_GET_IND_PG_VAL_LIQ(P_IDE_DMDEV IN ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR.IDEDMDEV%TYPE)
            RETURN ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR.INDPGTOTT%TYPE IS

        V_VAL_1207 NUMBER;
        V_VAL_SIGEPREV NUMBER;

    BEGIN
        --SOMA BRUTO 1207

        SELECT TRUNC(SUM(DECODE(D.DADOSRUBRICA_TPRUBR,1,C.VRRUBR,C.VRRUBR*-1)))
        INTO V_VAL_1207
        FROM TSOC_CPL_1207_DEMONSTRATIVO A,
             TSOC_CPL_1207_ORGAO_UNIDADE_N B,
             TSOC_CPL_1207_RUBRICA_N C,
             TSOC_1010_RUBRICA D
        WHERE A.IDEMDEV = P_IDE_DMDEV
          AND B.ID_DEMONSTRATIVO = A.ID_DEMONSTRATIVO
          AND C.ID_UNIDADE_N = B.ID_UNIDADE_N
          AND D.IDERUBRICA_CODRUBR = C.CODRUBR;

        --SOMA BRUTO ESOCIAL

        SELECT TRUNC(SUM(DECODE(HD.FLG_NATUREZA,
                                    'C', HD.VAL_RUBRICA,
                                         HD.VAL_RUBRICA * -1)))
        INTO V_VAL_SIGEPREV
        FROM USER_IPESP.TB_HFOLHA HF,
             USER_IPESP.TB_ESOCIAL_HDET_CALCULADO HD,
             USER_IPESP.TB_RUBRICAS R
        WHERE HD.COD_INS = 1
          AND HD.COD_BENEFICIO = GB_FOLHA.COD_BENEFICIO
          AND HD.COD_IDE_CLI = GB_FOLHA.COD_IDE_CLI
          AND HD.PER_PROCESSO = GB_FOLHA.PER_PROCESSO
          AND HD.PER_COMPETENCIA = GB_FOLHA.PER_COMPETENCIA
          AND HD.SEQ_PAGAMENTO = GB_FOLHA.SEQ_PAGAMENTO
          AND HD.TIP_PROCESSO = GB_FOLHA.TIP_PROCESSO
          AND HD.DAT_INI_REF = HD.PER_PROCESSO
          AND HF.COD_INS = HD.COD_INS
          AND HF.COD_BENEFICIO = HD.COD_BENEFICIO
          AND HF.COD_IDE_CLI = HD.COD_IDE_CLI
          AND HF.PER_PROCESSO = HD.PER_PROCESSO
          AND HF.SEQ_PAGAMENTO = HD.SEQ_PAGAMENTO
          AND HF.TIP_PROCESSO = HD.TIP_PROCESSO
          AND HF.COD_ENTIDADE = R.COD_ENTIDADE
          AND TRUNC(HD.COD_FCRUBRICA/100) NOT IN (70012,70014,70078,70081)
          AND R.COD_RUBRICA = HD.COD_FCRUBRICA
          AND R.TIP_EVENTO_ESPECIAL <> 'P';

        IF V_VAL_1207 > V_VAL_SIGEPREV THEN
            RETURN 'N';

        ELSIF V_VAL_1207 = V_VAL_SIGEPREV THEN
            RETURN 'S';

        ELSE
            RETURN 'X';
        END IF;


    END FC_GET_IND_PG_VAL_LIQ;



    FUNCTION FC_GET_QTDRUBR(P_COD_RUBRICA IN USER_IPESP.TB_RUBRICAS.COD_RUBRICA%TYPE)
       RETURN USER_IPESP.TB_COMPOSICAO_BEN.VAL_INIDADE%TYPE
       IS
       V_QTD_RUB USER_IPESP.TB_COMPOSICAO_BEN.VAL_INIDADE%TYPE;

       BEGIN
          SELECT E.VAL_INIDADE
            INTO V_QTD_RUB
            FROM USER_IPESP.TB_COMPOSICAO_BEN E
           WHERE E.COD_BENEFICIO = GB_FOLHA.COD_BENEFICIO
             AND E.COD_FCRUBRICA = P_COD_RUBRICA
             AND EXISTS ( SELECT 1 FROM USER_IPESP.TB_RUBRICAS R WHERE R.TIP_COMPOSICAO = 'B'
                             AND R.COD_RUBRICA = E.COD_FCRUBRICA
                        )
             AND ROWNUM = 1;

        RETURN V_QTD_RUB;

        EXCEPTION
          WHEN OTHERS THEN
            RETURN NULL;

    END FC_GET_QTDRUBR;


    FUNCTION FC_GET_FATOR_RUB(P_COD_RUBRICA IN USER_IPESP.TB_RUBRICAS.COD_RUBRICA%TYPE)
      RETURN USER_IPESP.TB_COMPOSICAO_BEN.VAL_PORC%TYPE IS
      V_VAL_PORC USER_IPESP.TB_COMPOSICAO_BEN.VAL_PORC%TYPE;
    BEGIN

      SELECT E.VAL_PORC
        INTO V_VAL_PORC
        FROM USER_IPESP.TB_COMPOSICAO_BEN E
       WHERE E.COD_BENEFICIO = GB_FOLHA.COD_BENEFICIO
         AND E.COD_FCRUBRICA = P_COD_RUBRICA
           AND EXISTS ( SELECT 1 FROM USER_IPESP.TB_RUBRICAS R
                          WHERE R.TIP_COMPOSICAO = 'B'
                            AND R.COD_RUBRICA = E.COD_FCRUBRICA
                        )
         AND ROWNUM = 1;

      RETURN V_VAL_PORC;

    EXCEPTION
      WHEN OTHERS THEN
        RETURN NULL;

    END FC_GET_FATOR_RUB;


    PROCEDURE SP_INC_1210_RET_PGTO_PR(P_RET_PGTO_PR IN ESOCIAL.TSOC_CPL_1210_RET_PGTO_TOT_PR%ROWTYPE ) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1210_RET_PGTO_TOT_PR
            (ID_RET_PGTO_TOT_PR,
             ID_PGTO_BEN_PR,
             CODRUBR,
             IDETABRUBR,
             QTDRUBR,
             FATORRUBR,
             VRUNIT,
             VRRUBR,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_RET_PGTO_PR.ID_RET_PGTO_TOT_PR,
             P_RET_PGTO_PR.ID_PGTO_BEN_PR,
             P_RET_PGTO_PR.CODRUBR,
             P_RET_PGTO_PR.IDETABRUBR,
             P_RET_PGTO_PR.QTDRUBR,
             P_RET_PGTO_PR.FATORRUBR,
             P_RET_PGTO_PR.VRUNIT,
             P_RET_PGTO_PR.VRRUBR,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_1210_RET_PGTO_PR');

        -- COMMIT;  -- v1.02

    END SP_INC_1210_RET_PGTO_PR;


    PROCEDURE SP_1210_RET_PGTO_PR(P_PGTO_BEN_PR ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR%ROWTYPE) IS

        V_RET_PGTO_PR ESOCIAL.TSOC_CPL_1210_RET_PGTO_TOT_PR%ROWTYPE;

    BEGIN

        FOR C_RUB_RET IN ( SELECT HD.COD_FCRUBRICA,
                                  HD.VAL_RUBRICA,
                                  TR.IDERUBRICA_IDETABRUBR
                           FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO HD, 
                                ESOCIAL.TSOC_CAD_RUBRICA     C,
                                ESOCIAL.TSOC_1010_RUBRICA TR
                           WHERE HD.COD_INS = GB_COD_INS
                             AND HD.PER_PROCESSO = GB_FOLHA.PER_PROCESSO
                             AND HD.PER_COMPETENCIA = GB_FOLHA.PER_COMPETENCIA
                             AND HD.COD_IDE_CLI = GB_FOLHA.COD_IDE_CLI
                             AND HD.COD_BENEFICIO = GB_FOLHA.COD_BENEFICIO
                             AND HD.SEQ_PAGAMENTO = GB_FOLHA.SEQ_PAGAMENTO
                             AND HD.PER_PROCESSO = HD.DAT_INI_REF
                             AND TRUNC(HD.COD_FCRUBRICA/100) IN (70012,70014,70078,70081)
                             AND EXISTS ( SELECT 1 FROM USER_IPESP.TB_RUBRICAS R
                                          WHERE R.COD_RUBRICA = HD.COD_FCRUBRICA
                                            AND R.TIP_EVENTO_ESPECIAL = 'P' )
                             AND TR.COD_INS = HD.COD_INS
                             --TT
                             AND TR.IDERUBRICA_CODRUBR = TO_CHAR(HD.COD_FCRUBRICA) 
                             AND TR.ID_CAD_RUB    = C.ID_CAD_RUB
                             AND TR.COD_INS       = C.COD_INS
                             --TT
                             AND TR.ID_ORIGEM           = 1 
                             --TICKET 79609 - Esocial SPPREV S-1207: Retorno de Erro: Código 933 Duplicidade de Rubricas por Tipo
              AND TR.IDERUBRICA_IDETABRUBR = 
              (SELECT  CASE
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idPenCiv'
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE = 5) THEN
                           'idPenMil'
                         WHEN (CB.COD_TIPO_BENEFICIO <> 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idAposen'
                         WHEN (CB.COD_TIPO_BENEFICIO IN('REFEO','REFAP') AND CB.COD_ENTIDADE = 5) THEN
                           'idRefor'
                         ELSE
                           'idReserv'
                       END
                FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB
                WHERE CB.COD_INS = HD.COD_INS
                  AND CB.COD_BENEFICIO = HD.COD_BENEFICIO
                  AND CB.COD_ENTIDADE = C.COD_ENTIDADE
                )          
                             /*AND EXISTS ( SELECT 1
                                          FROM TSOC_1010_RUBRICA TR
                                          WHERE TR.IDERUBRICA_CODRUBR = HD.COD_FCRUBRICA )*/ ) LOOP

            V_RET_PGTO_PR.ID_RET_PGTO_TOT_PR := ESOC_SEQ_ID_1210_RET_PG_TOT_PR.NEXTVAL;
            V_RET_PGTO_PR.ID_PGTO_BEN_PR     :=  P_PGTO_BEN_PR.ID_PGTO_BEN_PR;
            V_RET_PGTO_PR.CODRUBR            := C_RUB_RET.COD_FCRUBRICA;
            V_RET_PGTO_PR.IDETABRUBR         := C_RUB_RET.IDERUBRICA_IDETABRUBR;--'TB_RUBRI';
            V_RET_PGTO_PR.QTDRUBR            := FC_GET_QTDRUBR(C_RUB_RET.COD_FCRUBRICA);
            V_RET_PGTO_PR.FATORRUBR          := FC_GET_FATOR_RUB(C_RUB_RET.COD_FCRUBRICA);
            V_RET_PGTO_PR.VRUNIT             := C_RUB_RET.VAL_RUBRICA;
            V_RET_PGTO_PR.VRRUBR             := C_RUB_RET.VAL_RUBRICA;

            SP_INC_1210_RET_PGTO_PR(V_RET_PGTO_PR);

        END LOOP;

    END SP_1210_RET_PGTO_PR;


    PROCEDURE SP_INC_1210_DET_PGTO_BEN_PR(P_DET_PGTO_BEN_PR IN ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR%ROWTYPE) IS
    BEGIN
        INSERT INTO ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR
            (ID_PGTO_BEN_PR,
             ID_PGTO,
             PERREF,
             IDEDMDEV,
             INDPGTOTT,
             VRLIQ,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_DET_PGTO_BEN_PR.ID_PGTO_BEN_PR,
             P_DET_PGTO_BEN_PR.ID_PGTO,
             P_DET_PGTO_BEN_PR.PERREF,
             P_DET_PGTO_BEN_PR.IDEDMDEV,
             P_DET_PGTO_BEN_PR.INDPGTOTT,
             P_DET_PGTO_BEN_PR.VRLIQ,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_DET_PGTO_BEN_PR');

        --  COMMIT;     -- v1.02

    END SP_INC_1210_DET_PGTO_BEN_PR;


    PROCEDURE SP_INC_1210_INFO_PGTO_PARC_PR(P_INFO_PGTO_PARC_PR IN ESOCIAL.TSOC_CPL_1210_INFO_PG_PARC_PR%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_CPL_1210_INFO_PG_PARC_PR
            (ID_INFO_PGTO_PARC_PR,
             ID_PGTO_BEN_PR,
             CODRUBR,
             IDETABRUBR,
             QTDRUBR,
             FATORRUBR,
             VRUNIT,
             VRRUBR,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_INFO_PGTO_PARC_PR.ID_INFO_PGTO_PARC_PR,
            P_INFO_PGTO_PARC_PR.ID_PGTO_BEN_PR,
            P_INFO_PGTO_PARC_PR.CODRUBR,
            P_INFO_PGTO_PARC_PR.IDETABRUBR,
            P_INFO_PGTO_PARC_PR.QTDRUBR,
            P_INFO_PGTO_PARC_PR.FATORRUBR,
            P_INFO_PGTO_PARC_PR.VRUNIT,
            P_INFO_PGTO_PARC_PR.VRRUBR,
            SYSDATE,
            SYSDATE,
            USER,
            'SP_INC_1210_INFO_PGTO_PARC_PR');

        -- COMMIT;  -- v1.02

    END  SP_INC_1210_INFO_PGTO_PARC_PR;



    PROCEDURE SP_1210_INFO_PGTO_PARC_PR(P_1210_DET_PGTO_BEN_PR IN ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR%ROWTYPE) IS

        V_INFO_PGTO_PARC_PR ESOCIAL.TSOC_CPL_1210_INFO_PG_PARC_PR%ROWTYPE;

    BEGIN

        FOR C_RUB_RET IN ( SELECT HD.COD_FCRUBRICA,
                                  HD.VAL_RUBRICA,
                                  TR.IDERUBRICA_IDETABRUBR
                           FROM USER_IPESP.TB_ESOCIAL_HDET_CALCULADO HD, 
                           ESOCIAL.TSOC_CAD_RUBRICA     C,
                                ESOCIAL.TSOC_1010_RUBRICA TR
                           WHERE HD.COD_INS = GB_COD_INS
                             AND HD.PER_PROCESSO = GB_FOLHA.PER_PROCESSO
                             AND HD.PER_COMPETENCIA = GB_FOLHA.PER_COMPETENCIA
                             AND HD.COD_IDE_CLI = GB_FOLHA.COD_IDE_CLI
                             AND HD.COD_BENEFICIO = GB_FOLHA.COD_BENEFICIO
                             AND HD.SEQ_PAGAMENTO = GB_FOLHA.SEQ_PAGAMENTO
                             AND HD.PER_PROCESSO = HD.DAT_INI_REF
                             AND TRUNC(HD.COD_FCRUBRICA/100) IN (70012,70014,70078,70081)
                             AND EXISTS ( SELECT 1 FROM USER_IPESP.TB_RUBRICAS R
                                          WHERE R.COD_RUBRICA = HD.COD_FCRUBRICA
                                            AND R.TIP_EVENTO_ESPECIAL = 'P' )
                             AND TR.COD_INS = HD.COD_INS
                              --TT
                             AND TR.IDERUBRICA_CODRUBR = TO_CHAR(HD.COD_FCRUBRICA) 
                             AND TR.ID_CAD_RUB    = C.ID_CAD_RUB
                             AND TR.COD_INS       = C.COD_INS
                             --TT
                             AND TR.ID_ORIGEM           = 1 
                             --TICKET 79609 - Esocial SPPREV S-1207: Retorno de Erro: Código 933 Duplicidade de Rubricas por Tipo
              AND TR.IDERUBRICA_IDETABRUBR = 
              (SELECT  CASE
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idPenCiv'
                         WHEN (CB.COD_TIPO_BENEFICIO = 'M' AND CB.COD_ENTIDADE = 5) THEN
                           'idPenMil'
                         WHEN (CB.COD_TIPO_BENEFICIO <> 'M' AND CB.COD_ENTIDADE <> 5) THEN
                           'idAposen'
                         WHEN (CB.COD_TIPO_BENEFICIO IN('REFEO','REFAP') AND CB.COD_ENTIDADE = 5) THEN
                           'idRefor'
                         ELSE
                           'idReserv'
                       END
                FROM USER_IPESP.TB_CONCESSAO_BENEFICIO CB
                WHERE CB.COD_INS = HD.COD_INS
                  AND CB.COD_BENEFICIO = HD.COD_BENEFICIO
                  AND CB.COD_ENTIDADE = C.COD_ENTIDADE
                )                       
                             /*AND EXISTS ( SELECT 1
                                          FROM TSOC_1010_RUBRICA TR
                                          WHERE TR.IDERUBRICA_CODRUBR = HD.COD_FCRUBRICA )*/  ) LOOP

            V_INFO_PGTO_PARC_PR.ID_INFO_PGTO_PARC_PR := ESOCIAL.ESOC_SEQ_ID_1210_PG_PARC_PR.NEXTVAL;
            V_INFO_PGTO_PARC_PR.ID_PGTO_BEN_PR :=  P_1210_DET_PGTO_BEN_PR.ID_PGTO_BEN_PR;
            V_INFO_PGTO_PARC_PR.CODRUBR := C_RUB_RET.COD_FCRUBRICA;
            V_INFO_PGTO_PARC_PR.IDETABRUBR := C_RUB_RET.IDERUBRICA_IDETABRUBR;--'TB_RUBRI';
            V_INFO_PGTO_PARC_PR.QTDRUBR := FC_GET_QTDRUBR(C_RUB_RET.COD_FCRUBRICA);
            V_INFO_PGTO_PARC_PR.FATORRUBR := FC_GET_FATOR_RUB(C_RUB_RET.COD_FCRUBRICA);
            V_INFO_PGTO_PARC_PR.VRUNIT := C_RUB_RET.VAL_RUBRICA;
            V_INFO_PGTO_PARC_PR.VRRUBR := C_RUB_RET.VAL_RUBRICA;

            SP_INC_1210_INFO_PGTO_PARC_PR(V_INFO_PGTO_PARC_PR);

        END LOOP;

    END SP_1210_INFO_PGTO_PARC_PR;


    PROCEDURE SP_1210_DET_PGTO_BEN_PR(P_ID_PGTO       IN NUMBER,
                                      P_CAD_DET_FOLHA IN TSOC_CAD_DET_FOLHA%ROWTYPE) IS

        V_1210_DET_PGTO_BEN_PR ESOCIAL.TSOC_CPL_1210_DET_PGTO_BEN_PR%ROWTYPE;

    BEGIN

        V_1210_DET_PGTO_BEN_PR.ID_PGTO_BEN_PR := ESOC_SEQ_ID_1210_DET_PG_BEN_PR.NEXTVAL;
        V_1210_DET_PGTO_BEN_PR.ID_PGTO        := P_ID_PGTO;
        V_1210_DET_PGTO_BEN_PR.PERREF         := TO_CHAR(GB_PER_COMPETENCIA,'YYYY-MM');

        --Obtém o demonstrativo de pagamento no evento 1207
        V_1210_DET_PGTO_BEN_PR.IDEDMDEV := FC_GET_IDEDMDEV(P_CAD_DET_FOLHA.ID_CAD_FOLHA,
                                                           P_CAD_DET_FOLHA.COD_BENEFICIO,
                                                           P_CAD_DET_FOLHA.SEQ_PAGAMENTO);
        --Verifica se o pagamento é total ou parcial
        --V_1210_DET_PGTO_BEN_PR.INDPGTOTT := FC_GET_IND_PG_VAL_LIQ(V_1210_DET_PGTO_BEN_PR.IDEDMDEV);

        V_1210_DET_PGTO_BEN_PR.VRLIQ := P_CAD_DET_FOLHA.VAL_LIQUIDO;

        SP_INC_1210_DET_PGTO_BEN_PR(V_1210_DET_PGTO_BEN_PR);

        --Registra Rubricas de Retenção de pagamento
        --SP_1210_RET_PGTO_PR(V_1210_DET_PGTO_BEN_PR);

        --Se o pagamento foi parcial cadastrar dados de pagamento parcial
        /*IF V_1210_DET_PGTO_BEN_PR.INDPGTOTT = 'N' THEN
            SP_1210_INFO_PGTO_PARC_PR(V_1210_DET_PGTO_BEN_PR);
        END IF;*/

    END SP_1210_DET_PGTO_BEN_PR;


    PROCEDURE SP_INC_1210(P_1210 IN ESOCIAL.TSOC_1210_PAG_RENDIMENTOS%ROWTYPE) IS
    BEGIN

        INSERT INTO TSOC_1210_PAG_RENDIMENTOS
            ( ID_PK,
              ID_CAD_FOLHA,
              ID_ORIGEM,
              ID_LOTE,
              ID_EVENTO,
              ID_PERIODO_DET,
              ID,
              INDRETIF,
              NRRECIBO,
              --INDAPURACAO,
              PERAPUR,
              TPAMB,
              PROCEMI,
              VERPROC,
              TPINSC,
              NRINSC,
              CPFBENEF,
              --VRDEDDEP,
              SEQ_EVENTO,
              CTR_FLG_STATUS,
              XML_ENVIO,
              WS_COD_RESPOSTA,
              WS_DSC_RESPOSTA,
              WS_DH_PROC,
              WS_VER_APP_PROC,
              FLG_VIGENCIA,
              DAT_ING,
              DAT_ULT_ATU,
              NOM_USU_ULT_ATU,
              NOM_PRO_ULT_ATU,
              COD_INS,
              INDGUIA,
              DTPGTO,
              TPPGTO,
              PERREF,
              IDEDMDEV,
              VRLIQ)
        VALUES
            ( P_1210.ID_PK,
              P_1210.ID_CAD_FOLHA,
              P_1210.ID_ORIGEM,
              P_1210.ID_LOTE,
              P_1210.ID_EVENTO,
              P_1210.ID_PERIODO_DET,
              P_1210.ID,
              P_1210.INDRETIF,
              P_1210.NRRECIBO,
              --DALVES S1210 12/11/2021
              --P_1210.INDAPURACAO,
              P_1210.PERAPUR,
              P_1210.TPAMB,
              P_1210.PROCEMI,
              P_1210.VERPROC,
              P_1210.TPINSC,
              P_1210.NRINSC,
              P_1210.CPFBENEF,
              --DALVES S1210 12/11/2021
              --P_1210.VRDEDDEP,
              P_1210.SEQ_EVENTO,
              P_1210.CTR_FLG_STATUS,
              P_1210.XML_ENVIO,
              P_1210.WS_COD_RESPOSTA,
              P_1210.WS_DSC_RESPOSTA,
              P_1210.WS_DH_PROC,
              P_1210.WS_VER_APP_PROC,
              P_1210.FLG_VIGENCIA,
              SYSDATE,
              SYSDATE,
              USER,
              'SP_INC_1210',
              --DALVES S1210 12/11/2021
              P_1210.COD_INS,
              P_1210.INDGUIA,
              P_1210.DTPGTO,
              P_1210.TPPGTO,
              P_1210.PERREF,
              P_1210.IDEDMDEV,
              P_1210.VRLIQ);

        -- COMMIT;  -- v1.02

    END SP_INC_1210;


    PROCEDURE  SP_INC_1210_INFO_PGTO(P_1210_INFO_PGTO IN ESOCIAL.TSOC_CPL_1210_INFO_PGTO%ROWTYPE) IS
    BEGIN

        INSERT INTO  ESOCIAL.TSOC_CPL_1210_INFO_PGTO
            (ID_PGTO,
             ID_PK,
             DTPGTO,
             TPPGTO,
             INDRESBR,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU)
        VALUES
            (P_1210_INFO_PGTO.ID_PGTO,
             P_1210_INFO_PGTO.ID_PK,
             P_1210_INFO_PGTO.DTPGTO,
             P_1210_INFO_PGTO.TPPGTO,
             P_1210_INFO_PGTO.INDRESBR,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INC_INFO_PGTO');

        -- COMMIT;  -- v1.02

    END SP_INC_1210_INFO_PGTO;


    PROCEDURE SP_1210_INFO_PGTO(p_1210_pag_rend in     esocial.tsoc_1210_pag_rendimentos%rowtype,
                                o_retorno       in out varchar2) IS     -- v1.02

        V_1210_INFO_PGTO ESOCIAL.TSOC_CPL_1210_INFO_PGTO%ROWTYPE;

    BEGIN

        FOR C_DET_FOLHA IN ( SELECT * FROM TSOC_CAD_DET_FOLHA DF
                              WHERE DF.ID_CAD_FOLHA = P_1210_PAG_REND.ID_CAD_FOLHA ) LOOP
            BEGIN   -- v1.02

                V_1210_INFO_PGTO.ID_PGTO := ESOC_SEQ_ID_1210_INFO_PGTO.NEXTVAL;
                V_1210_INFO_PGTO.ID_PK   := P_1210_PAG_REND.ID_PK;
                V_1210_INFO_PGTO.DTPGTO  := C_DET_FOLHA.DT_FECHAMENTO;

                GB_FOLHA.COD_BENEFICIO   := C_DET_FOLHA.COD_BENEFICIO;
                GB_FOLHA.SEQ_PAGAMENTO   := C_DET_FOLHA.SEQ_PAGAMENTO;
                GB_FOLHA.TIP_PROCESSO    := C_DET_FOLHA.TIP_PROCESSO;

                /*Informar o tipo de pagamento, de acordo com as opções a seguir:
                1 - Pagamento de remuneração, conforme apurado em {dmDev} do S-1200;
                2 - Pagamento de verbas rescisórias conforme apurado em {dmDev} do S-2299;
                3 - Pagamento de verbas rescisórias conforme apurado em {dmDev} do S-2399;
                5 - Pagamento de remuneração conforme apurado em {dmDev} do S-1202;
                6 - Pagamento de benefícios, conforme apurado em {dmDev} do S-1207;
                7 - Recibo de férias;
                9 - Pagamento relativo a competências anteriores ao início de
                obrigatoriedade dos eventos periódicos para o contribuinte.
                Valores Válidos: 1, 2, 3, 5, 6, 7, 9.     */
                V_1210_INFO_PGTO.TPPGTO := 5;
               /* Atualmente todos os pagamentos gerados pelo do SIGEPREV,
                 são efetuados no Brasil, apesar do beneficiário residir no exterior,
                 por este motivo, utilizar sempre o valor ¿S¿ para todos os beneficiários.*/
                V_1210_INFO_PGTO.INDRESBR := 'S'; --VERIFICAR

                SP_INC_1210_INFO_PGTO(V_1210_INFO_PGTO);

                SP_1210_DET_PGTO_BEN_PR(V_1210_INFO_PGTO.ID_PGTO, C_DET_FOLHA );

            -- v1.02 - início
            EXCEPTION
                WHEN OTHERS THEN
                    GB_REC_ERRO.COD_INS           := GB_COD_INS;
                    GB_REC_ERRO.ID_CAD            := P_1210_PAG_REND.ID_CAD_FOLHA;
                    GB_REC_ERRO.NOM_PROCESSO      := 'SP_1210_INFO_PGTO';
                    GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                    GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
                    GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                    GB_REC_ERRO.DES_IDENTIFICADOR := P_1210_PAG_REND.ID_CAD_FOLHA;
                    GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
                    GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                    GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                    if (o_retorno is not null) then
                        o_retorno := substr(o_retorno || chr(10), 1, 4000);
                    end if;
                    o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
                    SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);

                    exit;
            end;
            -- v1.02 - fim

        END LOOP;

    -- v1.02 - início
    EXCEPTION
        WHEN OTHERS THEN
            GB_REC_ERRO.COD_INS           := GB_COD_INS;
            GB_REC_ERRO.ID_CAD            := P_1210_PAG_REND.ID_CAD_FOLHA;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1210_INFO_PGTO';
            GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := P_1210_PAG_REND.ID_CAD_FOLHA;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            if (o_retorno is not null) then
                o_retorno := substr(o_retorno || chr(10), 1, 4000);
            end if;
            o_retorno := substr(o_retorno || GB_REC_ERRO.DESC_ERRO, 1, 4000);
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
    -- v1.02 - fim

    END SP_1210_INFO_PGTO;


    FUNCTION FC_GET_DED_DEP_IR(P_COD_IDE_CLI IN USER_IPESP.TB_PESSOA_FISICA.COD_IDE_CLI%TYPE)
            RETURN NUMBER IS

        V_DED_DEP_IR NUMBER;

    BEGIN

        SELECT VAL_ELEMENTO * NUM_DEP_IR
        INTO V_DED_DEP_IR
        FROM USER_IPESP.TB_DET_PARAM_ESTRUTURA PE,
             USER_IPESP.TB_PESSOA_FISICA       PF
        WHERE COD_PARAM = 'IMPDE'
          AND FIM_VIG IS NULL
          AND PF.COD_IDE_CLI = P_COD_IDE_CLI;

      RETURN V_DED_DEP_IR;

    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;

    END FC_GET_DED_DEP_IR;


    -- v1.02 - início
    PROCEDURE SP_1210_PROC_RENDIMENTOS (i_1210    in     esocial.tsoc_cad_folha%rowtype,
                                        i_perapur in     esocial.tsoc_1210_pag_rendimentos.perapur%type,
                                        o_retorno in out varchar2) IS

        v_1210          esocial.tsoc_1210_pag_rendimentos%rowtype;

    BEGIN

        gb_folha.cod_ide_cli  := i_1210.cod_ide_cli;
        gb_folha.per_processo := i_1210.per_processo;
        GB_FOLHA.PER_COMPETENCIA := I_1210.PER_COMPETENCIA;

        v_1210.id             := fc_gera_id_evento;
        v_1210.id_pk          := esoc_seq_id_1210_pg_rend.nextval;
        v_1210.id_cad_folha   := i_1210.id_cad_folha;
        v_1210.id_origem      := gb_id_origem;
        v_1210.id_evento      := gb_id_evento;
        v_1210.id_periodo_det := gb_id_periodo_det;
        v_1210.indretif       := 1;
        v_1210.nrrecibo       := null;
        --DALVES 12/11/2021
        --v_1210.indapuracao    := 1; --mensal
        v_1210.perapur        := i_perapur;

        v_1210.tpamb          := gb_amb.tpamb;
        v_1210.procemi        := gb_amb.procemi;
        v_1210.verproc        := gb_amb.verproc;
        v_1210.tpinsc         := gb_empregador.tp_insc;
        v_1210.nrinsc         := gb_empregador.num_cnpj;

        v_1210.cpfbenef       := fc_cpf_ben(i_1210.cod_ide_cli);

        /*Valor da dedução da base de cálculo do IRRF relativo aos dependentes
        do beneficiário do pagamento, correspondente ao número de dependentes multiplicado
        pelo valor de dedução por dependente previsto na legislação do Imposto de Renda.*/
        --DALVES S1210 12/11/2021
        --v_1210.vrdeddep       := fc_get_ded_dep_ir(i_1210.cod_ide_cli); --verificar

        --nao tratar retificação. aguardar definições

        v_1210.indretif       := 1;
        v_1210.nrrecibo       := null;
        v_1210.ctr_flg_status := 'AX';
        v_1210.flg_vigencia   := 'A';
        v_1210.cod_ins        := gb_cod_ins;
        
         --DALVES S1210 12/11/2021
        V_1210.INDGUIA        := 1;
        V_1210.DTPGTO         := null;--i_1210.DTPGTO;  
        V_1210.TPPGTO         := null;--i_1210.tppgto;
        V_1210.PERREF         := null;--i_1210.PERREF;
        V_1210.IDEDMDEV       := null;--i_1210.IDEDMDEV;
        V_1210.VRLIQ          := null;--i_1210.VRLIQ;

        sp_inc_1210(v_1210);

        --gera informaçôes de pagamento
        sp_1210_info_pgto(v_1210, o_retorno);

    END SP_1210_PROC_RENDIMENTOS;
    -- v1.02 - fim


    PROCEDURE SP_1210_RENDIMENTOS(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS

        -- V_1210          ESOCIAL.TSOC_1210_PAG_RENDIMENTOS%ROWTYPE;   -- v1.02
        EX_PARAM_PROC   EXCEPTION;
        -- v1.02 - início
        v_perapur       esocial.tsoc_1210_pag_rendimentos.perapur%type;
        v_retorno       varchar2(4000);
        -- v1.02 - fim
        CURSOR C_CAD_FOLHA(P_PER_PROCESSO DATE, P_FAIXA_CPF_INI VARCHAR2, P_FAIXA_CPF_FIM VARCHAR2) IS
        SELECT CF.*
          FROM ESOCIAL.TSOC_CAD_FOLHA CF, USER_IPESP.TB_PESSOA_FISICA PF
         WHERE CF.COD_INS = PF.COD_INS
           AND CF.COD_IDE_CLI = PF.COD_IDE_CLI
           AND CF.PER_PROCESSO = P_PER_PROCESSO
           AND PF.NUM_CPF >= NVL(P_FAIXA_CPF_INI,PF.NUM_CPF)
           AND PF.NUM_CPF <= NVL(P_FAIXA_CPF_FIM,PF.NUM_CPF)
           AND NOT EXISTS (SELECT 1
                  FROM ESOCIAL.TSOC_1210_PAG_RENDIMENTOS PR
                 WHERE PR.ID_CAD_FOLHA = CF.ID_CAD_FOLHA)
                 /*AND ROWNUM <= 50000*/;

    BEGIN

        --Valida parametrização
        GB_ID_CTR_PROCESSO := P_ID_CTR_PROCESSO;

        BEGIN
            SP_CARREGA_IDS;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EX_PARAM_PROC;
        END;

        SP_SETA_PROCESSO('INICIO_PROCESSAMENTO');

        commit;     -- v1.02

        SP_SET_PER_PROCESSO;

        -- v1.01 - Início
        SP_RET_INFO_AMBIENTE;
        SP_RET_INSC_EMP;
        -- v1.01 - Fim

        -- v1.02 - Início
        --TT78544 - Esocial SPPREV S-1210: PerApur +1 Mês
        select to_char(add_months(to_date(p.periodo, 'MM/YYYY'),1), 'YYYY-MM')
        into v_perapur
        from esocial.tsoc_ctr_periodo_det pd,
             esocial.tsoc_ctr_periodo p
        where p.cod_ins         = gb_cod_ins
          and p.id_periodo      = pd.id_periodo
          and pd.id_periodo_det = gb_id_periodo_det;
        -- v1.02 - Fim

        FOR C_1210 IN C_CAD_FOLHA(GB_PER_COMPETENCIA,GB_FAIXA_INI_CPF,GB_FAIXA_FIM_CPF) LOOP

            BEGIN   -- v1.02

                -- v1.01 - início
                /*
                GB_DAT_EVT_ANT := GB_DAT_EVT_ATU;
                GB_DAT_EVT_ATU := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MMSS');

                IF GB_DAT_EVT_ATU = GB_DAT_EVT_ANT THEN
                  GB_SEQ_CHAVE_ID := GB_SEQ_CHAVE_ID + 1;
                ELSE
                  GB_SEQ_CHAVE_ID := 1;
                END IF;
                */
                -- v1.01 - Fim

                -- v1.02 - início
                /*
                GB_FOLHA.COD_IDE_CLI  := C_1210.COD_IDE_CLI;
                GB_FOLHA.PER_PROCESSO := C_1210.PER_PROCESSO;

                V_1210.ID             := FC_GERA_ID_EVENTO;
                V_1210.ID_PK          := ESOC_SEQ_ID_1210_PG_REND.NEXTVAL;
                V_1210.ID_CAD_FOLHA   := C_1210.ID_CAD_FOLHA;
                V_1210.ID_ORIGEM      := GB_ID_ORIGEM;
                V_1210.ID_EVENTO      := GB_ID_EVENTO;
                V_1210.ID_PERIODO_DET := GB_ID_PERIODO_DET;
                V_1210.INDRETIF       := 1;
                V_1210.NRRECIBO       := NULL;
                V_1210.INDAPURACAO    := 1; --MENSAL

                SELECT TO_CHAR(TO_DATE(P.PERIODO, 'MM/YYYY'), 'YYYY-MM')
                INTO V_1210.PERAPUR
                FROM ESOCIAL.TSOC_CTR_PERIODO_DET PD, ESOCIAL.TSOC_CTR_PERIODO P
                WHERE P.COD_INS         = 1
                  AND P.ID_PERIODO      = PD.ID_PERIODO
                  AND PD.ID_PERIODO_DET = GB_ID_PERIODO_DET;

                --SP_RET_INFO_AMBIENTE;  -- v1.01

                V_1210.TPAMB    := GB_AMB.TPAMB;
                V_1210.PROCEMI  := GB_AMB.PROCEMI;
                V_1210.VERPROC  := GB_AMB.VERPROC;

                -- SP_RET_INSC_EMP;  -- v1.01

                V_1210.TPINSC   := GB_EMPREGADOR.TP_INSC;
                V_1210.NRINSC   := GB_EMPREGADOR.NUM_CNPJ;

                V_1210.CPFBENEF := FC_CPF_BEN(C_1210.COD_IDE_CLI);

                \*Valor da dedução da base de cálculo do IRRF relativo aos dependentes
                do beneficiário do pagamento, correspondente ao número de dependentes multiplicado
                pelo valor de dedução por dependente previsto na legislação do Imposto de Renda.*\

                V_1210.VRDEDDEP := FC_GET_DED_DEP_IR(C_1210.COD_IDE_CLI); --VERIFICAR
                --NAO TRATAR RETIFICAÇÃO. AGUARDAR DEFINIÇÕES
                V_1210.INDRETIF       := 1;
                V_1210.NRRECIBO       := NULL;
                V_1210.CTR_FLG_STATUS := 'AX';
                V_1210.FLG_VIGENCIA   := 'A';
                V_1210.COD_INS        := GB_COD_INS;

                SP_INC_1210(V_1210);

                --GERA INFORMAÇôES DE PAGAMENTO
                SP_1210_INFO_PGTO(V_1210);
                */

                v_retorno := null;
                sp_1210_proc_rendimentos(C_1210, v_perapur, v_retorno);

                --SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');    --Atualiza Quantidade
                if (v_retorno is null) then
                    SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');    --Atualiza Quantidade
                    commit;
                else
                    rollback;
                end if;
                -- v1.02 - Fim

            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK;
                    GB_REC_ERRO.COD_INS           := GB_COD_INS;
                    GB_REC_ERRO.ID_CAD            := C_1210.ID_CAD_FOLHA;
                    GB_REC_ERRO.NOM_PROCESSO      := 'SP_1210_RENDIMENTOS';
                    GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                    GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
                    GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                    GB_REC_ERRO.DES_IDENTIFICADOR := C_1210.COD_IDE_CLI;
                    GB_REC_ERRO.FLG_TIPO_ERRO     := 'X'; --REGISTRO NÃO CONSTA NA TABELA
                    GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                    GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                    SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            END;
            -- v1.02 - fim
        END LOOP;

        SP_SETA_PROCESSO('FIM_PROCESSAMENTO');

        commit;     -- v1.02

    EXCEPTION
        WHEN EX_PARAM_PROC THEN
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1210_RENDIMENTOS';
            GB_REC_ERRO.ID_EVENTO         := NULL;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO NA PARAMETRIZAÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

        WHEN OTHERS THEN
            rollback;           -- v1.02
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1210_RENDIMENTOS';
            GB_REC_ERRO.ID_EVENTO         := NULL;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

    END SP_1210_RENDIMENTOS;


PROCEDURE SP_INSERE_1299(P_1299 IN ESOCIAL.TSOC_1299_FECHAMENTO_EP%ROWTYPE) IS
BEGIN

  INSERT INTO ESOCIAL.TSOC_1299_FECHAMENTO_EP
    (ID_PK,
     ID_ORIGEM,
     ID_LOTE,
     ID_EVENTO,
     ID_PERIODO_DET,
     ID,
     INDAPURACAO,
     PERAPUR,
     TPAMB,
     PROCEMI,
     VERPROC,
     TPINSC,
     NRINSC,
     NMRESP,
     CPFRESP,
     TELEFONE,
     EMAIL,
     EVTREMUN,
     EVTPGTOS,
     EVTAQPROD,
     EVTCOMPROD,
     EVTCONTRATAVNP,
     EVTINFOCOMPLPER,
     COMPSEMMOVTO,
     SEQ_EVENTO,
     CTR_FLG_STATUS,
     XML_ENVIO,
     WS_COD_RESPOSTA,
     WS_DSC_RESPOSTA,
     WS_DH_PROC,
     WS_VER_APP_PROC,
     FLG_VIGENCIA,
     DAT_ING,
     DAT_ULT_ATU,
     NOM_USU_ULT_ATU,
     NOM_PRO_ULT_ATU,
     COD_INS,
     ID_CAD_FECHAMENTO,
     INDEXCAPUR1250,
     TRANSDCTFWEB,
     NAOVALID)

  VALUES

    (P_1299.ID_PK,
     P_1299.ID_ORIGEM,
     P_1299.ID_LOTE,
     P_1299.ID_EVENTO,
     P_1299.ID_PERIODO_DET,
     P_1299.ID,
     P_1299.INDAPURACAO,
     P_1299.PERAPUR,
     P_1299.TPAMB,
     P_1299.PROCEMI,
     P_1299.VERPROC,
     P_1299.TPINSC,
     P_1299.NRINSC,
     P_1299.NMRESP,
     P_1299.CPFRESP,
     P_1299.TELEFONE,
     P_1299.EMAIL,
     P_1299.EVTREMUN,
     P_1299.EVTPGTOS,
     P_1299.EVTAQPROD,
     P_1299.EVTCOMPROD,
     P_1299.EVTCONTRATAVNP,
     P_1299.EVTINFOCOMPLPER,
     P_1299.COMPSEMMOVTO,
     P_1299.SEQ_EVENTO,
     P_1299.CTR_FLG_STATUS,
     P_1299.XML_ENVIO,
     P_1299.WS_COD_RESPOSTA,
     P_1299.WS_DSC_RESPOSTA,
     P_1299.WS_DH_PROC,
     P_1299.WS_VER_APP_PROC,
     P_1299.FLG_VIGENCIA,
     SYSDATE,
     SYSDATE,
     USER,
     'SP_INSERE_1299',
     P_1299.COD_INS,
     P_1299.ID_CAD_FECHAMENTO,
     P_1299.INDEXCAPUR1250,
     P_1299.TRANSDCTFWEB,
     P_1299.NAOVALID);

  -- COMMIT;  -- v1.02

END SP_INSERE_1299;

    PROCEDURE SP_INSERE_1298(P_1298 IN ESOCIAL.TSOC_1298_REABERTURA_EP%ROWTYPE) IS
    BEGIN

        INSERT INTO ESOCIAL.TSOC_1298_REABERTURA_EP
            (ID_PK,
             ID_ORIGEM,
             ID_LOTE,
             ID_EVENTO,
             ID_PERIODO_DET,
             ID,
             INDAPURACAO,
             PERAPUR,
             TPAMB,
             PROCEMI,
             VERPROC,
             TPINSC,
             NRINSC,
             NMRESP,
             CPFRESP,
             TELEFONE,
             EMAIL,
             EVTREMUN,
             EVTPGTOS,
             EVTAQPROD,
             EVTCOMPROD,
             EVTCONTRATAVNP,
             EVTINFOCOMPLPER,
             COMPSEMMOVTO,
             SEQ_EVENTO,
             CTR_FLG_STATUS,
             XML_ENVIO,
             WS_COD_RESPOSTA,
             WS_DSC_RESPOSTA,
             WS_DH_PROC,
             WS_VER_APP_PROC,
             FLG_VIGENCIA,
             DAT_ING,
             DAT_ULT_ATU,
             NOM_USU_ULT_ATU,
             NOM_PRO_ULT_ATU,
             COD_INS,
             ID_CAD_REABERTURA,
             INDGUIA)

        VALUES
            (P_1298.ID_PK,
             P_1298.ID_ORIGEM,
             P_1298.ID_LOTE,
             P_1298.ID_EVENTO,
             P_1298.ID_PERIODO_DET,
             P_1298.ID,
             P_1298.INDAPURACAO,
             P_1298.PERAPUR,
             P_1298.TPAMB,
             P_1298.PROCEMI,
             P_1298.VERPROC,
             P_1298.TPINSC,
             P_1298.NRINSC,
             P_1298.NMRESP,
             P_1298.CPFRESP,
             P_1298.TELEFONE,
             P_1298.EMAIL,
             P_1298.EVTREMUN,
             P_1298.EVTPGTOS,
             P_1298.EVTAQPROD,
             P_1298.EVTCOMPROD,
             P_1298.EVTCONTRATAVNP,
             P_1298.EVTINFOCOMPLPER,
             P_1298.COMPSEMMOVTO,
             P_1298.SEQ_EVENTO,
             P_1298.CTR_FLG_STATUS,
             P_1298.XML_ENVIO,
             P_1298.WS_COD_RESPOSTA,
             P_1298.WS_DSC_RESPOSTA,
             P_1298.WS_DH_PROC,
             P_1298.WS_VER_APP_PROC,
             P_1298.FLG_VIGENCIA,
             SYSDATE,
             SYSDATE,
             USER,
             'SP_INSERE_1298',
             P_1298.COD_INS,
             P_1298.ID_CAD_REABERTURA,
             P_1298.INDGUIA);

        -- COMMIT;  -- v1.02

    END SP_INSERE_1298;


    PROCEDURE SP_1299_PROC_FECHAMENTO_PE(i_rec_cad_fechamento in     esocial.tsoc_cad_fechamento_ep%rowtype,
                                         o_retorno            in out varchar2) IS

        v_1299          esocial.tsoc_1299_fechamento_ep%rowtype;

    BEGIN
      
      SELECT DECODE(COUNT(*),1,'S','N')
        INTO v_1299.evtremun
        FROM ESOCIAL.TSOC_CTR_PERIODO_DET
       WHERE ID_PERIODO IN
             (SELECT CP.ID_PERIODO
                FROM ESOCIAL.TSOC_CTR_PERIODO CP
               WHERE CP.FLG_STATUS = 'A'
                 AND CP.PERIODO =
                     NVL2(SUBSTR(i_rec_cad_fechamento.periodo_fechamento, 6, 2),
                          SUBSTR(i_rec_cad_fechamento.periodo_fechamento, 6, 2) || '/' || 
                          SUBSTR(i_rec_cad_fechamento.periodo_fechamento, 1, 4),
                          SUBSTR(i_rec_cad_fechamento.periodo_fechamento, 1, 4)))
         AND ID_EVENTO = 8;

        v_1299.nmresp            := i_rec_cad_fechamento.nome_responsavel;
        v_1299.cpfresp           := i_rec_cad_fechamento.cpf_responsavel;
        v_1299.telefone          := i_rec_cad_fechamento.telefone;
        v_1299.email             := i_rec_cad_fechamento.email;
        --v_1299.evtremun          := i_rec_cad_fechamento.flg_remuneracao;
        v_1299.evtpgtos          := i_rec_cad_fechamento.flg_pagamento;
        v_1299.evtaqprod         := i_rec_cad_fechamento.flg_aq_prod_rural;
        v_1299.evtcomprod        := i_rec_cad_fechamento.flg_com_prod;
        v_1299.evtcontratavnp    := i_rec_cad_fechamento.flg_contrat_sind_np;
        v_1299.evtinfocomplper   := i_rec_cad_fechamento.flg_info_compl_per;
        v_1299.compsemmovto      := i_rec_cad_fechamento.per_comp_sem_mov;
        v_1299.cod_ins           := i_rec_cad_fechamento.cod_ins;
        v_1299.id_cad_fechamento := i_rec_cad_fechamento.id_cad_fechamento;
        v_1299.indapuracao       := i_rec_cad_fechamento.tipo_periodo;
        v_1299.perapur           := i_rec_cad_fechamento.periodo_fechamento;
        v_1299.id_pk             := esoc_seq_id_1299.nextval;
        v_1299.id_origem         := gb_id_origem;
        v_1299.id_evento         := gb_id_evento;
        v_1299.id_periodo_det    := gb_id_periodo_det;
        v_1299.flg_vigencia      := 'A';
        v_1299.id                := fc_gera_id_evento;
        v_1299.tpamb             := gb_amb.tpamb;
        v_1299.procemi           := gb_amb.procemi;
        v_1299.verproc           :=  gb_amb.verproc;
        v_1299.tpinsc            := gb_empregador.tp_insc;
        v_1299.nrinsc            := gb_empregador.num_cnpj;
        v_1299.seq_evento        := 1;
        v_1299.ctr_flg_status    := 'AX';
        v_1299.indExcApur1250    := '';
        v_1299.transDCTFWeb      := '';
        v_1299.naoValid          := 'S';

        sp_insere_1299(v_1299);

    EXCEPTION
        WHEN OTHERS THEN
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_rec_cad_fechamento.id_cad_fechamento;
            gb_rec_erro.nom_processo      := 'SP_1299_FECHAMENTO_PE';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_rec_cad_fechamento.id_cad_fechamento;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            gb_rec_erro.id_ctr_processo   := gb_id_ctr_processo;
            if (o_retorno is not null) then
                o_retorno := substr(o_retorno || chr(10), 1, 4000);
            end if;
            o_retorno := substr(o_retorno || gb_rec_erro.desc_erro, 1, 4000);
            sp_gera_erro_processo_at(gb_rec_erro);

    END SP_1299_PROC_FECHAMENTO_PE;

    PROCEDURE SP_1298_PROC_REABERTURA_PE(i_rec_cad_reabertura in     esocial.tsoc_cad_reabertura_ep%rowtype,
                                         o_retorno            in out varchar2) IS

        v_1298          esocial.tsoc_1298_reabertura_ep%rowtype;

    BEGIN

        v_1298.nmresp            := i_rec_cad_reabertura.nome_responsavel;
        v_1298.cpfresp           := i_rec_cad_reabertura.cpf_responsavel;
        v_1298.telefone          := i_rec_cad_reabertura.telefone;
        v_1298.email             := i_rec_cad_reabertura.email;
        v_1298.evtremun          := i_rec_cad_reabertura.flg_remuneracao;
        v_1298.evtpgtos          := i_rec_cad_reabertura.flg_pagamento;
        v_1298.evtaqprod         := i_rec_cad_reabertura.flg_aq_prod_rural;
        v_1298.evtcomprod        := i_rec_cad_reabertura.flg_com_prod;
        v_1298.evtcontratavnp    := i_rec_cad_reabertura.flg_contrat_sind_np;
        v_1298.evtinfocomplper   := i_rec_cad_reabertura.flg_info_compl_per;
        v_1298.compsemmovto      := i_rec_cad_reabertura.per_comp_sem_mov;
        v_1298.cod_ins           := i_rec_cad_reabertura.cod_ins;
        v_1298.id_cad_reabertura := i_rec_cad_reabertura.id_cad_reabertura;
        v_1298.indapuracao       := i_rec_cad_reabertura.tipo_periodo;
        v_1298.perapur           := i_rec_cad_reabertura.periodo_reabertura;
        v_1298.id_pk             := esoc_seq_id_1298.nextval;
        v_1298.id_origem         := gb_id_origem;
        v_1298.id_evento         := gb_id_evento;
        v_1298.id_periodo_det    := gb_id_periodo_det;
        v_1298.flg_vigencia      := 'A';
        v_1298.id                := fc_gera_id_evento;
        v_1298.tpamb             := gb_amb.tpamb;
        v_1298.procemi           := gb_amb.procemi;
        v_1298.verproc           :=  gb_amb.verproc;
        v_1298.tpinsc            := gb_empregador.tp_insc;
        v_1298.nrinsc            := gb_empregador.num_cnpj;
        v_1298.seq_evento        := 1;
        v_1298.ctr_flg_status    := 'AX';

        sp_insere_1298(v_1298);

    EXCEPTION
        WHEN OTHERS THEN
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_rec_cad_reabertura.id_cad_reabertura;
            gb_rec_erro.nom_processo      := 'SP_1298_PROC_REABERTURA_PE';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_rec_cad_reabertura.id_cad_reabertura;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;
            gb_rec_erro.id_ctr_processo   := gb_id_ctr_processo;
            if (o_retorno is not null) then
                o_retorno := substr(o_retorno || chr(10), 1, 4000);
            end if;
            o_retorno := substr(o_retorno || gb_rec_erro.desc_erro, 1, 4000);
            sp_gera_erro_processo_at(gb_rec_erro);

    END SP_1298_PROC_REABERTURA_PE;


    PROCEDURE SP_1299_FECHAMENTO_PE(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS

        -- V_1299          ESOCIAL.TSOC_1299_FECHAMENTO_EP%ROWTYPE;     -- v1.02
        EX_PARAM_PROC   EXCEPTION;
        v_retorno       varchar2(4000);

    BEGIN

        --Valida parametrização
        GB_ID_CTR_PROCESSO := P_ID_CTR_PROCESSO;
        BEGIN
            SP_CARREGA_IDS;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EX_PARAM_PROC;
        END;

        SP_SETA_PROCESSO('INICIO_PROCESSAMENTO');

        commit;     -- v1.02

        -- v1.01 - Início
        SP_RET_INFO_AMBIENTE;
        SP_RET_INSC_EMP;
        -- v1.01 - Fim

        FOR C_FECHAMENTO IN ( SELECT *
                              FROM ESOCIAL.TSOC_CAD_FECHAMENTO_EP FE
                              WHERE NOT EXISTS ( SELECT 1
                                                 FROM ESOCIAL.TSOC_1299_FECHAMENTO_EP EPV
                                                 WHERE EPV.ID_CAD_FECHAMENTO = FE.ID_CAD_FECHAMENTO  ) ) LOOP

            -- v1.02 - início
            BEGIN
                /*
                V_1299.NMRESP            := C_FECHAMENTO.NOME_RESPONSAVEL;
                V_1299.CPFRESP           := C_FECHAMENTO.CPF_RESPONSAVEL;
                V_1299.TELEFONE          := C_FECHAMENTO.TELEFONE;
                V_1299.EMAIL             := C_FECHAMENTO.EMAIL;
                V_1299.EVTREMUN          := C_FECHAMENTO.FLG_REMUNERACAO;
                V_1299.EVTPGTOS          := C_FECHAMENTO.FLG_PAGAMENTO;
                V_1299.EVTAQPROD         := C_FECHAMENTO.FLG_AQ_PROD_RURAL;
                V_1299.EVTCOMPROD        := C_FECHAMENTO.FLG_COM_PROD;
                V_1299.EVTCONTRATAVNP    := C_FECHAMENTO.FLG_CONTRAT_SIND_NP;
                V_1299.EVTINFOCOMPLPER   := C_FECHAMENTO.FLG_INFO_COMPL_PER;
                V_1299.COMPSEMMOVTO      := C_FECHAMENTO.PER_COMP_SEM_MOV;
                V_1299.COD_INS           := C_FECHAMENTO.COD_INS;
                V_1299.ID_CAD_FECHAMENTO := C_FECHAMENTO.ID_CAD_FECHAMENTO;
                V_1299.INDAPURACAO       := C_FECHAMENTO.TIPO_PERIODO;
                V_1299.PERAPUR           := C_FECHAMENTO.PERIODO_FECHAMENTO;

                V_1299.ID_PK             := ESOC_SEQ_ID_1299.NEXTVAL;
                V_1299.ID_ORIGEM         := GB_ID_ORIGEM;
                V_1299.ID_EVENTO         := GB_ID_EVENTO;
                V_1299.ID_PERIODO_DET    := GB_ID_PERIODO_DET;
                V_1299.FLG_VIGENCIA      := 'A';

                -- v1.01 - Início
                \*
                GB_DAT_EVT_ANT := GB_DAT_EVT_ATU;
                GB_DAT_EVT_ATU := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MMSS');

                IF GB_DAT_EVT_ATU = GB_DAT_EVT_ANT THEN
                  GB_SEQ_CHAVE_ID := GB_SEQ_CHAVE_ID + 1;
                ELSE
                  GB_SEQ_CHAVE_ID := 1;
                END IF;
                *\
                -- v1.01 - Fim

                V_1299.ID             := FC_GERA_ID_EVENTO;

                -- v1.01 - Início
                --SP_RET_INFO_AMBIENTE;
                -- v1.01 - Fim
                V_1299.TPAMB   := GB_AMB.TPAMB;
                V_1299.PROCEMI := GB_AMB.PROCEMI;
                V_1299.VERPROC :=  GB_AMB.VERPROC;

                -- v1.01 - Início
                --SP_RET_INSC_EMP;
                -- v1.01 - Fim

                V_1299.TPINSC         := GB_EMPREGADOR.TP_INSC;
                V_1299.NRINSC         := GB_EMPREGADOR.NUM_CNPJ;
                V_1299.SEQ_EVENTO     := 1;
                V_1299.CTR_FLG_STATUS := 'AX';

                SP_INSERE_1299(V_1299);

                --Atualiza Quantidade
                SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');
                */
                v_retorno := null;
                SP_1299_PROC_FECHAMENTO_PE(C_FECHAMENTO, v_retorno);

                if (v_retorno is null) then

                    --Atualiza Quantidade
                    SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');

                    commit;

                else

                    rollback;

                end if;

            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK;
                    GB_REC_ERRO.COD_INS           := GB_COD_INS;
                    GB_REC_ERRO.ID_CAD            := C_FECHAMENTO.ID_CAD_FECHAMENTO;
                    GB_REC_ERRO.NOM_PROCESSO      := 'SP_1299_FECHAMENTO_PE';
                    GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                    GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
                    GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                    GB_REC_ERRO.DES_IDENTIFICADOR := C_FECHAMENTO.ID_CAD_FECHAMENTO;
                    GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                    GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                    -- SP_GERA_ERRO_PROCESSO;
                    GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                    SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);

                    continue;
            end;
            -- v1.02 - fim

        END LOOP;

        SP_SETA_PROCESSO('FIM_PROCESSAMENTO');

        commit;     -- v1.02

    EXCEPTION
        WHEN EX_PARAM_PROC THEN
            ROLLBACK;
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1299_FECHAMENTO_PE';
            GB_REC_ERRO.ID_EVENTO         := NULL;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO NA PARAMETRIZAÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

        WHEN OTHERS THEN
            ROLLBACK;
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1299_FECHAMENTO_PE';
            GB_REC_ERRO.ID_EVENTO         := NULL;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

    END SP_1299_FECHAMENTO_PE;

    --S-1298 - Reabertura dos Eventos Periódicos
    PROCEDURE SP_1298_REABERTURA_PE(P_ID_CTR_PROCESSO IN ESOCIAL.TSOC_CTR_PROCESSO.ID_CTR_PROCESSO%TYPE) IS

        -- V_1298          ESOCIAL.TSOC_1298_REABERTURA_EP%ROWTYPE;     -- v1.02
        EX_PARAM_PROC   EXCEPTION;
        v_retorno       varchar2(4000);

    BEGIN

        --Valida parametrização
        GB_ID_CTR_PROCESSO := P_ID_CTR_PROCESSO;
        BEGIN
            SP_CARREGA_IDS;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EX_PARAM_PROC;
        END;

        SP_SETA_PROCESSO('INICIO_PROCESSAMENTO');

        commit;     -- v1.02

        -- v1.01 - Início
        SP_RET_INFO_AMBIENTE;
        SP_RET_INSC_EMP;
        -- v1.01 - Fim

        FOR C_REABERTURA IN ( SELECT *
                              FROM ESOCIAL.TSOC_CAD_REABERTURA_EP FE
                              WHERE NOT EXISTS ( SELECT 1
                                                 FROM ESOCIAL.TSOC_1298_REABERTURA_EP EPV
                                                 WHERE EPV.ID_CAD_REABERTURA = FE.ID_CAD_REABERTURA  ) ) LOOP

            -- v1.02 - início
            BEGIN
                /*
                V_1299.NMRESP            := C_FECHAMENTO.NOME_RESPONSAVEL;
                V_1299.CPFRESP           := C_FECHAMENTO.CPF_RESPONSAVEL;
                V_1299.TELEFONE          := C_FECHAMENTO.TELEFONE;
                V_1299.EMAIL             := C_FECHAMENTO.EMAIL;
                V_1299.EVTREMUN          := C_FECHAMENTO.FLG_REMUNERACAO;
                V_1299.EVTPGTOS          := C_FECHAMENTO.FLG_PAGAMENTO;
                V_1299.EVTAQPROD         := C_FECHAMENTO.FLG_AQ_PROD_RURAL;
                V_1299.EVTCOMPROD        := C_FECHAMENTO.FLG_COM_PROD;
                V_1299.EVTCONTRATAVNP    := C_FECHAMENTO.FLG_CONTRAT_SIND_NP;
                V_1299.EVTINFOCOMPLPER   := C_FECHAMENTO.FLG_INFO_COMPL_PER;
                V_1299.COMPSEMMOVTO      := C_FECHAMENTO.PER_COMP_SEM_MOV;
                V_1299.COD_INS           := C_FECHAMENTO.COD_INS;
                V_1299.ID_CAD_FECHAMENTO := C_FECHAMENTO.ID_CAD_FECHAMENTO;
                V_1299.INDAPURACAO       := C_FECHAMENTO.TIPO_PERIODO;
                V_1299.PERAPUR           := C_FECHAMENTO.PERIODO_FECHAMENTO;

                V_1299.ID_PK             := ESOC_SEQ_ID_1299.NEXTVAL;
                V_1299.ID_ORIGEM         := GB_ID_ORIGEM;
                V_1299.ID_EVENTO         := GB_ID_EVENTO;
                V_1299.ID_PERIODO_DET    := GB_ID_PERIODO_DET;
                V_1299.FLG_VIGENCIA      := 'A';

                -- v1.01 - Início
                \*
                GB_DAT_EVT_ANT := GB_DAT_EVT_ATU;
                GB_DAT_EVT_ATU := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MMSS');

                IF GB_DAT_EVT_ATU = GB_DAT_EVT_ANT THEN
                  GB_SEQ_CHAVE_ID := GB_SEQ_CHAVE_ID + 1;
                ELSE
                  GB_SEQ_CHAVE_ID := 1;
                END IF;
                *\
                -- v1.01 - Fim

                V_1299.ID             := FC_GERA_ID_EVENTO;

                -- v1.01 - Início
                --SP_RET_INFO_AMBIENTE;
                -- v1.01 - Fim
                V_1299.TPAMB   := GB_AMB.TPAMB;
                V_1299.PROCEMI := GB_AMB.PROCEMI;
                V_1299.VERPROC :=  GB_AMB.VERPROC;

                -- v1.01 - Início
                --SP_RET_INSC_EMP;
                -- v1.01 - Fim

                V_1299.TPINSC         := GB_EMPREGADOR.TP_INSC;
                V_1299.NRINSC         := GB_EMPREGADOR.NUM_CNPJ;
                V_1299.SEQ_EVENTO     := 1;
                V_1299.CTR_FLG_STATUS := 'AX';

                SP_INSERE_1299(V_1299);

                --Atualiza Quantidade
                SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');
                */
                v_retorno := null;
                SP_1298_PROC_REABERTURA_PE(C_REABERTURA, v_retorno);

                if (v_retorno is null) then

                    --Atualiza Quantidade
                    SP_SETA_PROCESSO('ATUALIZA_QUANTIDADE');

                    commit;

                else

                    rollback;

                end if;

            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK;
                    GB_REC_ERRO.COD_INS           := GB_COD_INS;
                    GB_REC_ERRO.ID_CAD            := C_REABERTURA.ID_CAD_REABERTURA;
                    GB_REC_ERRO.NOM_PROCESSO      := 'SP_1299_FECHAMENTO_PE';
                    GB_REC_ERRO.ID_EVENTO         := GB_ID_EVENTO;
                    GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
                    GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
                    GB_REC_ERRO.DES_IDENTIFICADOR := C_REABERTURA.ID_CAD_REABERTURA;
                    GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
                    GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                    -- SP_GERA_ERRO_PROCESSO;
                    GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
                    SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);

                    continue;
            end;
            -- v1.02 - fim

        END LOOP;

        SP_SETA_PROCESSO('FIM_PROCESSAMENTO');

        commit;     -- v1.02

    EXCEPTION
        WHEN EX_PARAM_PROC THEN
            ROLLBACK;
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1299_FECHAMENTO_PE';
            GB_REC_ERRO.ID_EVENTO         := NULL;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO NA PARAMETRIZAÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

        WHEN OTHERS THEN
            ROLLBACK;
            GB_REC_ERRO.COD_INS           := NULL;
            GB_REC_ERRO.ID_CAD            := NULL;
            GB_REC_ERRO.NOM_PROCESSO      := 'SP_1299_FECHAMENTO_PE';
            GB_REC_ERRO.ID_EVENTO         := NULL;
            GB_REC_ERRO.DESC_ERRO         := 'ERRO DURANTE A EXECUÇÃO DO PROCESSO';
            GB_REC_ERRO.DESC_ERRO_BD      := SQLERRM;
            GB_REC_ERRO.DES_IDENTIFICADOR := NULL;
            GB_REC_ERRO.FLG_TIPO_ERRO     := 'X';
            GB_REC_ERRO.DET_ERRO          := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            -- v1.02 - início
            -- SP_GERA_ERRO_PROCESSO;
            GB_REC_ERRO.ID_CTR_PROCESSO   := GB_ID_CTR_PROCESSO;
            SP_GERA_ERRO_PROCESSO_AT(GB_REC_ERRO);
            -- v1.02 - fim

            SP_SETA_PROCESSO('ERRO_PROCESSAMENTO');

            commit;     -- v1.02

    END SP_1298_REABERTURA_PE;


    PROCEDURE SP_ALT_1207_INDIVIDUAL(i_id_cad_folha in     esocial.tsoc_cad_folha.id_cad_folha%type,
                                     o_retorno      in out varchar2) IS

        -- Essa rotina não faz commit. Quem é responsável por isso é a rotina chamadora
        v_alt_cad_folha     esocial.tsoc_cad_folha%rowtype;
        erro                exception;
        v_retorno           varchar2(4000);

    BEGIN

        savepoint sp1;

        sp_default_session;

        if (i_id_cad_folha is null) then
            v_retorno             := 'Erro: Chave de identificação da pessoa física não informada';
            gb_rec_erro.desc_erro := 'CHAVE DE IDENTIFICAÇÃO DA PESSOA FÍSICA NÃO INFORMADA';
            raise erro;
        end if;

        -- Obtém dados de TSOC_CAD_FOLHA
        v_retorno             := 'Erro: Não foi possível obter os dados de TSOC_CAD_FOLHA';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DE TSOC_CAD_FOLHA';

        select *
        into v_alt_cad_folha
        from esocial.tsoc_cad_folha
        where id_cad_folha = i_id_cad_folha;

        -- Obtém dados do evento
        v_retorno             := 'Erro: Não foi possível obter os dados do evento';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO EVENTO';

        sp_carrega_ids_evento('1207');

        -- Define período
        v_retorno             := 'Erro: Não foi possível obter os dados do período';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO PERÍODO';

        sp_set_per_processo;

        -- Obtém dados do ambiente
        v_retorno             := 'Erro: Não foi possível obter os dados do ambiente';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO AMBIENTE';

        sp_ret_info_ambiente;

        -- Obtém dados do empregador
        v_retorno             := 'Erro: Não foi possível obter os dados do empregador';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO EMPREGADOR';

        sp_ret_insc_emp;

        gb_rec_erro.desc_erro := 'ERRO AO TENTAR GERAR EVENTO DE ALTERACAO INDIVIDUAL DE PAGAMENTO DE BENEFICIO';
        GB_IND_RETIF := 1;
                        
        v_retorno             := null;

        sp_1207_beneficio(v_alt_cad_folha, v_retorno); -- Executa sp_1207_beneficio sem fazer commit

        if (v_retorno is null) then
            o_retorno := 'OK';
        else
            rollback to sp1;
            o_retorno := v_retorno;
        end if;

    EXCEPTION

        when erro then
            rollback to sp1;
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_id_cad_folha;
            gb_rec_erro.nom_processo      := 'SP_ALT_1207_INDIVIDUAL';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_id_cad_folha;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;

            sp_gera_erro_processo_at(gb_rec_erro);

            o_retorno                     := v_retorno;

        when others then
            rollback to sp1;
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_id_cad_folha;
            gb_rec_erro.nom_processo      := 'SP_ALT_1207_INDIVIDUAL';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_id_cad_folha;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;

            sp_gera_erro_processo_at(gb_rec_erro);

            o_retorno                     := substr(v_retorno||': '||sqlerrm, 1, 1000);

    END SP_ALT_1207_INDIVIDUAL;


    PROCEDURE SP_ALT_1210_INDIVIDUAL(i_id_cad_folha in     esocial.tsoc_cad_folha.id_cad_folha%type,
                                     o_retorno      in out varchar2) IS

        -- Essa rotina não faz commit. Quem é responsável por isso é a rotina chamadora
        v_alt_cad_folha     esocial.tsoc_cad_folha%rowtype;
        v_perapur           esocial.tsoc_1210_pag_rendimentos.perapur%type;
        erro                exception;
        v_retorno           varchar2(4000);

    BEGIN

        savepoint sp1;

        sp_default_session;

        if (i_id_cad_folha is null) then
            v_retorno             := 'Erro: Chave de identificação da pessoa física não informada';
            gb_rec_erro.desc_erro := 'CHAVE DE IDENTIFICAÇÃO DA PESSOA FÍSICA NÃO INFORMADA';
            raise erro;
        end if;

        -- Obtém dados de TSOC_CAD_FOLHA
        v_retorno             := 'Erro: Não foi possível obter os dados de TSOC_CAD_FOLHA';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DE TSOC_CAD_FOLHA';

        select *
        into v_alt_cad_folha
        from esocial.tsoc_cad_folha
        where id_cad_folha = i_id_cad_folha;

        -- Obtém dados do evento
        v_retorno             := 'Erro: Não foi possível obter os dados do evento';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO EVENTO';

        sp_carrega_ids_evento('1210');

        -- Define período
        v_retorno             := 'Erro: Não foi possível obter os dados do período';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO PERÍODO';

        sp_set_per_processo;

        -- Obtém dados do ambiente
        v_retorno             := 'Erro: Não foi possível obter os dados do ambiente';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO AMBIENTE';

        sp_ret_info_ambiente;

        -- Obtém dados do empregador
        v_retorno             := 'Erro: Não foi possível obter os dados do empregador';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO EMPREGADOR';

        sp_ret_insc_emp;

        -- Obtém período do evento
        v_retorno             := 'Erro: Não foi possível obter os dados de período do evento';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DE PERÍODO DO EVENTO';

        select to_char(to_date(p.periodo, 'MM/YYYY'), 'YYYY-MM')
        into v_perapur
        from esocial.tsoc_ctr_periodo_det pd,
             esocial.tsoc_ctr_periodo p
        where p.cod_ins         = gb_cod_ins
          and p.id_periodo      = pd.id_periodo
          and pd.id_periodo_det = gb_id_periodo_det;

        gb_rec_erro.desc_erro := 'ERRO AO TENTAR GERAR EVENTO DE ALTERACAO INDIVIDUAL DE PAGAMENTO DE BENEFICIO';
        v_retorno             := null;

        sp_1210_proc_rendimentos(v_alt_cad_folha, v_perapur, v_retorno); -- Executa sp_1210_proc_rendimentos sem fazer commit

        if (v_retorno is null) then
            o_retorno := 'OK';
        else
            rollback to sp1;
            o_retorno := v_retorno;
        end if;

    EXCEPTION

        when erro then
            rollback to sp1;
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_id_cad_folha;
            gb_rec_erro.nom_processo      := 'SP_ALT_1210_INDIVIDUAL';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_id_cad_folha;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;

            sp_gera_erro_processo_at(gb_rec_erro);

            o_retorno                     := v_retorno;

        when others then
            rollback to sp1;
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_id_cad_folha;
            gb_rec_erro.nom_processo      := 'SP_ALT_1210_INDIVIDUAL';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_id_cad_folha;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;

            sp_gera_erro_processo_at(gb_rec_erro);

            o_retorno                     := substr(v_retorno||': '||sqlerrm, 1, 1000);

    END SP_ALT_1210_INDIVIDUAL;


    PROCEDURE SP_ALT_1299_INDIVIDUAL(i_id_cad_fechamento in     esocial.tsoc_cad_fechamento_ep.id_cad_fechamento%type,
                                     o_retorno           in out varchar2) IS

        -- Essa rotina não faz commit. Quem é responsável por isso é a rotina chamadora
        v_alt_cad_fechamento    esocial.tsoc_cad_fechamento_ep%rowtype;
        erro                    exception;
        v_retorno               varchar2(4000);

    BEGIN

        savepoint sp1;

        sp_default_session;

        if (i_id_cad_fechamento is null) then
            v_retorno             := 'Erro: Chave de identificação do registro de fechamento não informada';
            gb_rec_erro.desc_erro := 'CHAVE DE IDENTIFICAÇÃO DO REGISTRO DE FECHAMENTO NÃO INFORMADA';
            raise erro;
        end if;

        -- Obtém dados de TSOC_CAD_FOLHA
        v_retorno             := 'Erro: Não foi possível obter os dados de TSOC_CAD_FECHAMENTO_EP';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DE TSOC_CAD_FECHAMENTO_EP';

        select *
        into v_alt_cad_fechamento
        from esocial.tsoc_cad_fechamento_ep
        where id_cad_fechamento = i_id_cad_fechamento;

        -- Obtém dados do evento
        v_retorno             := 'Erro: Não foi possível obter os dados do evento';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO EVENTO';

        sp_carrega_ids_evento('1299');

        -- Define período
        v_retorno             := 'Erro: Não foi possível obter os dados do período';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO PERÍODO';

        sp_set_per_processo;

        -- Obtém dados do ambiente
        v_retorno             := 'Erro: Não foi possível obter os dados do ambiente';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO AMBIENTE';

        sp_ret_info_ambiente;

        -- Obtém dados do empregador
        v_retorno             := 'Erro: Não foi possível obter os dados do empregador';
        gb_rec_erro.desc_erro := 'ERRO AO TENTAR OBTER DADOS DO EMPREGADOR';

        sp_ret_insc_emp;

        gb_rec_erro.desc_erro := 'ERRO AO TENTAR GERAR EVENTO DE ALTERACAO INDIVIDUAL DE FECHAMENTO';
        v_retorno             := null;

        SP_1299_PROC_FECHAMENTO_PE(v_alt_cad_fechamento, v_retorno);

        if (v_retorno is null) then
            o_retorno := 'OK';
        else
            rollback to sp1;
            o_retorno := v_retorno;
        end if;

    EXCEPTION

        when erro then
            rollback to sp1;
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_id_cad_fechamento;
            gb_rec_erro.nom_processo      := 'SP_ALT_1210_INDIVIDUAL';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_id_cad_fechamento;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;

            sp_gera_erro_processo_at(gb_rec_erro);

            o_retorno                     := v_retorno;

        when others then
            rollback to sp1;
            gb_rec_erro.cod_ins           := gb_cod_ins;
            gb_rec_erro.id_cad            := i_id_cad_fechamento;
            gb_rec_erro.nom_processo      := 'SP_ALT_1210_INDIVIDUAL';
            gb_rec_erro.id_evento         := gb_id_evento;
            gb_rec_erro.desc_erro_bd      := sqlerrm;
            gb_rec_erro.des_identificador := i_id_cad_fechamento;
            gb_rec_erro.flg_tipo_erro     := 'X';
            gb_rec_erro.det_erro          := dbms_utility.format_error_backtrace;

            sp_gera_erro_processo_at(gb_rec_erro);

            o_retorno                     := substr(v_retorno||': '||sqlerrm, 1, 1000);

    END SP_ALT_1299_INDIVIDUAL;

END PAC_ESOCIAL_EVENTOS_PE_102;

/
