SELECT 
    stop_id,
    case when json_value(content, '$.IDPropriedadeParada') is null then false else true end flag_vistoriada,
    json_value(content, '$.stop_name') as stop_name,
    json_value(content, '$.stop_desc') as stop_desc,
    json_value(content, '$.stop_lat') as stop_lat,
    json_value(content, '$.stop_lon') as stop_lon,
    json_value(content, '$.location_type') as location_type,
    json_value(content, '$.GrupoLatitude') as GrupoLatitude,
    json_value(content, '$.GrupoLongitude') as GrupoLongitude,
    json_value(content, '$.idModalSmtr') as idModalSmtr,
    json_value(content, '$.IDCorredor') as IDCorredor,
    json_value(content, '$.IDTipoParada') as IDTipoParada,
    json_value(content, '$.IDTipoSentido') as IDTipoSentido,
    json_value(content, '$.IDPropriedadeParada') as IDPropriedadeParada,
    json_value(content, '$.Seletivado') as Seletivado,
    json_value(content, '$.BRS') as BRS,
    json_value(content, '$.IDTipoSinalizacao') as IDTipoSinalizacao,
    json_value(content, '$.IDConservacaoSinalizacao') as IDConservacaoSinalizacao,
    json_value(content, '$.IDTipoAbrigo') as IDTipoAbrigo,
    json_value(content, '$.ConservacaoAbrigo') as ConservacaoAbrigo,
    json_value(content, '$.IDTipoAssento') as IDTipoAssento,
    json_value(content, '$.IDTipoBaia') as IDTipoBaia,
    json_value(content, '$.QualidadePavimento') as QualidadePavimento,
    json_value(content, '$.IDTipoCalcada') as IDTipoCalcada,
    json_value(content, '$.IDLixeiras') as IDLixeiras,
    json_value(content, '$.IDRampa') as IDRampa,
    json_value(content, '$.braile') as braile,
    json_value(content, '$.piso_tatil') as piso_tatil,
    json_value(content, '$.elevador') as elevador,
    json_value(content, '$.n_vagas') as n_vagas,
    json_value(content, '$.n_cabines') as n_cabines,
    json_value(content, '$.AP') as AP,
    json_value(content, '$.RA') as RA,
    json_value(content, '$.Bairro') as Bairro,
    json_value(content, '$.Observacoes') as Observacoes,
    json_value(content, '$.dataAtualizacao') as dataAtualizacao,
    json_value(content, '$.endereco') as endereco,
    json_value(content, '$.IDBairro') as IDBairro,
    json_value(content, '$.IDRa') as IDRa,
    json_value(content, '$.MultiModal') as MultiModal,
    json_value(content, '$.id_sequencial') as id_sequencial,
    json_value(content, '$.NumeroLinha') as NumeroLinha,
    json_value(content, '$.Vista') as Vista,
    json_value(content, '$.Horarios') as Horarios,
    json_value(content, '$.id') as id,
    json_value(content, '$.PontoExistente') as PontoExistente
FROM {{ stops }} t
where data_versao = (select max(data_versao) FROM {{ stops }})
and json_value(content, '$.PontoExistente') = 'SIM'
and json_value(content, '$.idModalSmtr') = '22' or json_value(content, '$.idModalSmtr') = 'O'
