USE [master]
GO
/****** Object:  Database [SPI_DATA_GATHERING]    Script Date: 03/01/2019 15:18:43 ******/
CREATE DATABASE [SPI_DATA_GATHERING]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'SPI_DATA_GATHERING', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL12.SQLEXPRESS\MSSQL\DATA\SPI_DATA_GATHERING.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'SPI_DATA_GATHERING_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL12.SQLEXPRESS\MSSQL\DATA\SPI_DATA_GATHERING_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET COMPATIBILITY_LEVEL = 120
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [SPI_DATA_GATHERING].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET ARITHABORT OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET  DISABLE_BROKER 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET  MULTI_USER 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET DB_CHAINING OFF 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET DELAYED_DURABILITY = DISABLED 
GO
USE [SPI_DATA_GATHERING]
GO
/****** Object:  Table [dbo].[SPI_TB_DG_ARGUMENTS]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SPI_TB_DG_ARGUMENTS](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[ID_TRANS] [bigint] NOT NULL,
	[POS] [int] NOT NULL,
	[ID_TAG] [bigint] NULL,
	[CAMPO_OPCADDRESS] [nvarchar](50) NULL,
	[VL_FIXO] [nvarchar](50) NULL
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[SPI_TB_DG_CHANEL]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SPI_TB_DG_CHANEL](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Ip] [varchar](20) NULL,
	[Ch] [varchar](50) NULL,
	[En] [varchar](50) NULL,
	[Rng] [varchar](50) NULL,
	[Val] [varchar](50) NULL,
	[EgF] [varchar](50) NULL,
	[Evt] [varchar](50) NULL,
	[LoA] [varchar](50) NULL,
	[HiA] [varchar](50) NULL,
	[HVal] [varchar](50) NULL,
	[HEgF] [varchar](50) NULL,
	[LVal] [varchar](50) NULL,
	[LEgF] [varchar](50) NULL,
	[SVal] [varchar](50) NULL,
	[ClrH] [varchar](50) NULL,
	[ClrL] [varchar](50) NULL,
	[PEgF] [varchar](50) NULL,
	[Uni] [varchar](50) NULL,
	[Md] [varchar](50) NULL,
	[Stat] [varchar](50) NULL,
	[Cnting] [varchar](50) NULL,
	[OvLch] [varchar](50) NULL,
	[Changed_At] [datetime] NOT NULL,
 CONSTRAINT [PK_SPI_TB_CHANEL] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SPI_TB_DG_ERROR]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SPI_TB_DG_ERROR](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Date] [datetime] NOT NULL,
	[Mensagem] [varchar](1000) NOT NULL,
	[ProcedureName] [varchar](100) NOT NULL,
 CONSTRAINT [PK_SPI_TB_IL_ERROR] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SPI_TB_DG_TRANSACTIONS]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SPI_TB_DG_TRANSACTIONS](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[TriggerTagId] [bigint] NOT NULL,
	[TriggerValue] [varchar](50) NULL,
	[ProcedureName] [varchar](50) NOT NULL,
	[Enable] [bit] NOT NULL,
 CONSTRAINT [PK_SPI_TB_IL_TRANSACTIONS] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[SPI_TB_DG_WISE]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[SPI_TB_DG_WISE](
	[ID_WISE] [bigint] IDENTITY(1,1) NOT NULL,
	[IP] [varchar](100) NOT NULL,
	[CHANEL] [int] NOT NULL,
	[URL] [varchar](200) NOT NULL,
	[ENABLE] [bit] NOT NULL,
	[STATUS] [varchar](5) NULL,
	[DT_STATUS] [datetime] NULL,
 CONSTRAINT [PK_SPI_TB_DG_WISE] PRIMARY KEY CLUSTERED 
(
	[ID_WISE] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  StoredProcedure [dbo].[SPI_SP_COLETA_PORTA_SALA_PINTURA]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		THAIS LIMA DIAS
-- Create date: 18/12/2018
-- Description:	Procedure que é chamada pelo SPI_Data_Gathering Service para coletar PORTA ABERTA OU FECHADA dos sensores.
-- =============================================
CREATE PROCEDURE [dbo].[SPI_SP_COLETA_PORTA_SALA_PINTURA]
	  @Ip AS VARCHAR(20),
	  @Chanel AS SMALLINT,
	  @Por AS VARCHAR(20)
AS

BEGIN
	--DECLARAÇÃO DE VARIAVEIS DE APOIO
		DECLARE @ERRO AS NVARCHAR(MAX)
		DECLARE @IdSensor AS BIGINT = 0
		DECLARE @IdLocalMedicao AS BIGINT = 0
		DECLARE @DescLocalMed AS VARCHAR(80)
		DECLARE @Etapa AS VARCHAR(30)	
		DECLARE @IdAlarme AS BIGINT = 0
		DECLARE @DtInicio AS DATETIME
		DECLARE @DtFim AS DATETIME
		DECLARE @Msg AS NVARCHAR(max)
		DECLARE @Sts AS NVARCHAR(50)
		DECLARE @StsPor AS NVARCHAR(50)
		DECLARE @DtReconhecimento AS DATETIME
		DECLARE @DtJustificativa AS DATETIME
		DECLARE @DtWarning AS DATETIME
		DECLARE @DescPorta AS VARCHAR(30)
		DECLARE @Descricao AS VARCHAR(30)

		------------para inserir alarme E COLETA
		DECLARE @InsertAlarme AS BIT = 0
		DECLARE @IdCadParametroSistema AS BIGINT
		DECLARE @DescParametro AS VARCHAR(30)
		DECLARE @ControleMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMax AS DECIMAL (18,2)
		DECLARE @ControleMax AS DECIMAL (18,2)
		DECLARE @MsgTempoAlarme AS VARCHAR(MAX)
		DECLARE @MsgAlarme AS VARCHAR(MAX)
		DECLARE @TempoAlarmeMinutos AS SMALLINT

		

		BEGIN TRY
			--DEFINE O ID DE SENSOR E LOCAL DE MEDIÇÃO PARA INSERIR A MEDIÇÃO DE PRESSÃO
				SELECT @IdSensor = IdSensores,@Descricao=Descricao, @IdLocalMedicao = IdLocalMedicao 
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_SENSORES WHERE Ip = @Ip AND Canal=@Chanel
		
			--DEFINE A DESC DO LOCAL DE MEDIÇÃO
				SELECT @DescLocalMed = DescLocalMedicao
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_LOCAL_MEDICAO WHERE @IdLocalMedicao = IdLocalMedicao 

				IF @IdSensor=0 BEGIN
					-- SETANDO ERRO DE NÃO EXISTE SENSOR CADASTRADO PARA ESTE IP E CANAL RELATADO
						SET @ERRO = 'Não existe sensor Cadastrado para Este IP: ' + @Ip + ' Canal: ' + @Chanel
						INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_PORTA_SALA_PINTURA]')
						GOTO FIM				
				END	

				-- Parametros de Pressão
				SELECT @IdCadParametroSistema=IdCadParametroSistema,@DescParametro=DescParametro,@ControleMin=ControleMin,
				@EspecificacaoMin=EspecificacaoMin,@EspecificacaoMax=EspecificacaoMax,@ControleMax=ControleMax,
				@TempoAlarmeMinutos=TempoAlarmeMinutos,@MsgTempoAlarme=MsgTempoAlarme,@MsgAlarme=MsgAlarme
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_CADASTRO_PARAMETROS] 
				WHERE IdLocalMedicao = @IdLocalMedicao and DescParametro ='PINTURA F65 - Porta'



				-- CHECAGEM DE VALORES PARA CRIAÇÃO OU FINALIZAÇÃO DE ALARMES JÁ EXISTENTES				
				-- VERIFICA SE HÁ ALARMES EXISTENTES
				SELECT @IdAlarme=IdAlarme, @DtInicio=DtInicio, @DtFim=DtFim , @Msg =Mensagem,@Sts=StatusAlarme, 
						@DtJustificativa=DtJustificativa,@DtReconhecimento=DtReconhecimento
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
				WHERE IdLocalMedicao = @IdLocalMedicao AND TipoAlarme='Porta' AND StatusAlarme='Ativo'

				--VERIFICA SE HÁ WARNING ABERTO
				SELECT @DtWarning = DtInicio
					FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PORTA] 
					WHERE IdSensores = @IdSensor AND DtFim IS NULL	

				
				--Definindo nome da porta
				SET @DescPorta= 'Porta '+ SUBSTRING(@Descricao,8,1)

				--CONDIÇÕES PARA CASOS AONDE EXISTE UM ALARME
				IF @IdAlarme<>0 BEGIN
					-- CONFERE SE A PORTA ESTA ABERTA POR MAIS TEMPO QUE O ESPECIFICADO
					IF @Por = 0 BEGIN  						
						IF (DATEDIFF(MINUTE,GETDATE(),@DtInicio))>@TempoAlarmeMinutos BEGIN
							SET @Msg=@MsgTempoAlarme
						END	
						ELSE BEGIN				
							SET @Msg=@MsgAlarme	
						END						
					END
					-- CONFERE SE A PORTA ESTA FECHADA
					ELSE BEGIN
						--ENCERRA O ALARME ABERTO DE ACORDO COM O QUE FOI PREENCHIDO PELO OPERADOR NOS CAMPOS DE INTERAÇÃO
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()

						--ENCERRA O WARNING DE PORTA ABERTA
						UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PORTA] SET DtFim=GETDATE() 
						WHERE IdSensores = @IdSensor AND DtFim IS NULL	
					END
											
					--ATUALIZA O ALARME EXISTENTE COM O STATUS E MENSAGEM RESPECTIVA
					UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES  SET DtFim=@DtFim, StatusAlarme=@Sts, Mensagem = @Msg  WHERE IdAlarme=@IdAlarme													
				END
				--CONDIÇÕES PARA CASOS AONDE NÃO EXISTE  UM ALARME
				ELSE IF @IdAlarme=0 BEGIN
					IF (@DtWarning IS NULL AND @Por =0) BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PORTA] (IdSensores,Porta,DtInicio) VALUES ( @IdSensor,@Por,GETDATE())
					END
					ELSE BEGIN						
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ABAIXO DO ESPECIFICADO POR MAIS TEMPO QUE O PERMITIDO
						IF @Por = 0 BEGIN		
							IF (DATEDIFF(MINUTE,GETDATE(),@DtWarning))>@TempoAlarmeMinutos BEGIN
								SET @Msg=@MsgTempoAlarme
								SET @InsertAlarme = 1
							END	
							ELSE BEGIN				
								SET @Msg=@MsgAlarme	
							END							
						END 
						ELSE BEGIN
							--ENCERRA O WARNING DE PORTA ABERTA
							UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PORTA] SET DtFim=GETDATE() 
							WHERE IdSensores = @IdSensor AND DtFim IS NULL	
						END						
						IF @InsertAlarme = 1 BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
								(IdLocalMedicao,TipoAlarme,DtInicio,Mensagem,IdCadParametroSistema,ControleMin,EspecificacaoMin,EspecificacaoMax,ControleMax,StatusAlarme) 
								VALUES (@IdLocalMedicao,'Porta',GETDATE(),@Msg,@IdCadParametroSistema,@ControleMin,@EspecificacaoMin,@EspecificacaoMax,@ControleMax,'Ativo')
						END
					END
				END
				
				---Seta status 'Aberta' e 'Fechada'
				IF @Por=0 BEGIN
					SET @StsPor = 'Aberta'
				END
				ELSE BEGIN
					SET @StsPor = 'Fechada'
				END

				INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_COLETA_PORTA
					(IdLocalColeta,DtColeta,Valor,IdSensores,DescPorta)
					VALUES (@IdLocalMedicao,GETDATE(),@StsPor,@IdSensor,@DescPorta)
			
		END TRY

		BEGIN CATCH
			SET @ERRO = ERROR_PROCEDURE() + ': Line(' + CONVERT(varchar,ERROR_LINE()) + ') # ' + ERROR_MESSAGE()
			INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_PORTA_SALA_PINTURA]')
			PRINT(@ERRO)
		END CATCH

		FIM:
			PRINT('FIM')
END
GO
/****** Object:  StoredProcedure [dbo].[SPI_SP_COLETA_PRESSAO_SALA_PINTURA]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		THAIS LIMA DIAS
-- Create date: 18/12/2018
-- Description:	Procedure que é chamada pelo SPI_Data_Gathering Service para coletar PRESSÃO dos sensores.
-- =============================================
CREATE PROCEDURE [dbo].[SPI_SP_COLETA_PRESSAO_SALA_PINTURA]
	  @Ip AS VARCHAR(20),
	  @Chanel AS SMALLINT,
	  @Pre AS VARCHAR(20),
	  @UnidMedida AS VARCHAR(15)
AS

BEGIN
	--DECLARAÇÃO DE VARIAVEIS DE APOIO
		DECLARE @ERRO AS NVARCHAR(MAX)
		DECLARE @IdSensor AS BIGINT = 0
		DECLARE @IdLocalMedicao AS BIGINT = 0
		DECLARE @DescLocalMed AS VARCHAR(80)
		DECLARE @Etapa AS VARCHAR(30)	
		DECLARE @IdAlarme AS BIGINT = 0
		DECLARE @DtInicio AS DATETIME
		DECLARE @DtFim AS DATETIME
		DECLARE @Msg AS NVARCHAR(max)
		DECLARE @Sts AS NVARCHAR(50)
		DECLARE @DtReconhecimento AS DATETIME
		DECLARE @DtJustificativa AS DATETIME
		DECLARE @DtWarning AS DATETIME

		------------para inserir alarme E COLETA
		DECLARE @InsertAlarme AS BIT = 0
		DECLARE @IdCadParametroSistema AS BIGINT
		DECLARE @DescParametro AS VARCHAR(30)
		DECLARE @ControleMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMax AS DECIMAL (18,2)
		DECLARE @ControleMax AS DECIMAL (18,2)
		DECLARE @MsgTempoAlarme AS VARCHAR(MAX)
		DECLARE @MsgAlarme AS VARCHAR(MAX)
		DECLARE @TempoAlarmeMinutos AS SMALLINT

		

		BEGIN TRY
			--DEFINE O ID DE SENSOR E LOCAL DE MEDIÇÃO PARA INSERIR A MEDIÇÃO DE PRESSÃO
				SELECT @IdSensor = IdSensores, @IdLocalMedicao = IdLocalMedicao 
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_SENSORES WHERE Ip = @Ip AND Canal=@Chanel
		
			--DEFINE A DESC DO LOCAL DE MEDIÇÃO
				SELECT @DescLocalMed = DescLocalMedicao
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_LOCAL_MEDICAO WHERE @IdLocalMedicao = IdLocalMedicao 

				IF @IdSensor=0 BEGIN
					-- SETANDO ERRO DE NÃO EXISTE SENSOR CADASTRADO PARA ESTE IP E CANAL RELATADO
						SET @ERRO = 'Não existe sensor Cadastrado para Este IP: ' + @Ip + ' Canal: ' + @Chanel
						INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_PRESSAO_SALA_PINTURA]')
						GOTO FIM				
				END	

				-- Parametros de Pressão
				SELECT @IdCadParametroSistema=IdCadParametroSistema,@DescParametro=DescParametro,@ControleMin=ControleMin,
				@EspecificacaoMin=EspecificacaoMin,@EspecificacaoMax=EspecificacaoMax,@ControleMax=ControleMax,
				@TempoAlarmeMinutos=TempoAlarmeMinutos,@MsgTempoAlarme=MsgTempoAlarme,@MsgAlarme=MsgAlarme
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_CADASTRO_PARAMETROS] 
				WHERE IdLocalMedicao = @IdLocalMedicao and DescParametro ='PINTURA F65 - Pressão'



				-- CHECAGEM DE VALORES PARA CRIAÇÃO OU FINALIZAÇÃO DE ALARMES JÁ EXISTENTES				
				-- VERIFICA SE HÁ ALARMES EXISTENTES
				SELECT @IdAlarme=IdAlarme, @DtInicio=DtInicio, @DtFim=DtFim , @Msg =Mensagem,@Sts=StatusAlarme, 
						@DtJustificativa=DtJustificativa,@DtReconhecimento=DtReconhecimento
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
				WHERE IdLocalMedicao = @IdLocalMedicao AND TipoAlarme='Pressão' AND StatusAlarme='Ativo'

				--VERIFICA SE HÁ WARNING ABERTO
				SELECT @DtWarning = DtInicio
					FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PRESSAO] 
					WHERE IdSensores = @IdSensor AND DtFim IS NULL	

				
				--CONDIÇÕES PARA CASOS AONDE EXISTE UM ALARME
				IF @IdAlarme<>0 BEGIN
					--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTÁ ABAIXO DO ESPECIFICADO MINIMO POR MAIS TEMPO QUE O PERMITIDO SETANDO A MENSAGEM DE ALARME DE BAIXA UMIDADE
					IF @Pre < @EspecificacaoMin BEGIN  						
						IF (DATEDIFF(MINUTE,GETDATE(),@DtInicio))>@TempoAlarmeMinutos BEGIN
							SET @Msg=@MsgAlarme	
						END	
						ELSE BEGIN				
							SET @Msg=@MsgTempoAlarme
						END						
					END
					--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTÁ DENTRO DO ESPECIFICADO MINIMO E MÁXIMO ENCERRANDO ALARME DE BAIXA UMIDADE
					ELSE IF @Pre >= @EspecificacaoMin AND @Pre <= @EspecificacaoMax  BEGIN
						SET @Etapa = SUBSTRING(@DescParametro,CHARINDEX('-', @DescParametro, 9)+2,LEN(@DescParametro))	
						--ENCERRA O ALARME ABERTO DE ACORDO COM O QUE FOI PREENCHIDO PELO OOPERADOR NOS CAMPOS DE INTERAÇÃO
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()

						--ENCERRA O WARNING DE TEMPERATURA FORA DO ESPECIFICADO
						UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PRESSAO] SET DtFim=GETDATE() 
						WHERE IdSensores = @IdSensor AND DtFim IS NULL		
					END
					--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTÁ ACIMA DO ESPECIFICADO MAXIMO POR MAIS TEMPO QUE O PERMITIDO SETANDO A MENSAGEM DE ALARME DE ALTA UMIDADE
					IF @Pre > @EspecificacaoMax BEGIN
						IF (DATEDIFF(MINUTE,GETDATE(),@DtInicio))>@TempoAlarmeMinutos BEGIN
							SET @Msg=@MsgAlarme	
							SET @InsertAlarme = 1
						END	
						ELSE BEGIN				
							SET @Msg=@MsgTempoAlarme
						END							
					END
						
					--ATUALIZA O ALARME EXISTENTE COM O STATUS E MENSAGEM RESPECTIVA
					UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES  SET DtFim=@DtFim, StatusAlarme=@Sts, Mensagem = @Msg  WHERE IdAlarme=@IdAlarme													
				END
				--CONDIÇÕES PARA CASOS AONDE NÃO EXISTE  UM ALARME
				ELSE IF @IdAlarme=0 BEGIN
					IF (@DtWarning IS NULL AND @Pre < @EspecificacaoMin) BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PRESSAO] (IdSensores,Pressao,DtInicio) VALUES ( @IdSensor,@Pre,GETDATE())
					END
					ELSE BEGIN						
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ABAIXO DO ESPECIFICADO POR MAIS TEMPO QUE O PERMITIDO
						IF @Pre < @EspecificacaoMin BEGIN		
							IF (DATEDIFF(MINUTE,GETDATE(),@DtWarning))>@TempoAlarmeMinutos BEGIN
								SET @Msg=@MsgAlarme	
								SET @InsertAlarme = 1
							END	
							ELSE BEGIN				
								SET @Msg=@MsgTempoAlarme
							END	
								
						END 		
						--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTA COM O VALOR DENTRO  DO ESPECIFICADO 
						ELSE IF @Pre >= @EspecificacaoMin  AND @Pre <= @EspecificacaoMax BEGIN										
							--ENCERRA O WARNING QUE ESTÁ ABERTO
							UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_PRESSAO] SET DtFim=GETDATE() 
							WHERE IdSensores = @IdSensor AND DtFim IS NULL		
						END	
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ACIMA DO ESPECIFICADO POR MAIS TEMPO QUE O PERMITIDO
						ELSE IF @Pre > @EspecificacaoMax BEGIN		
							IF (DATEDIFF(MINUTE,GETDATE(),@DtWarning))>@TempoAlarmeMinutos BEGIN
								SET @Msg=@MsgAlarme	
								SET @InsertAlarme = 1
							END	
							ELSE BEGIN				
								SET @Msg=@MsgTempoAlarme
							END	
								
						END 					

						IF @InsertAlarme = 1 BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
								(IdLocalMedicao,TipoAlarme,DtInicio,Mensagem,IdCadParametroSistema,ControleMin,EspecificacaoMin,EspecificacaoMax,ControleMax,StatusAlarme) 
								VALUES (@IdLocalMedicao,'Pressão',GETDATE(),@Msg,@IdCadParametroSistema,@ControleMin,@EspecificacaoMin,@EspecificacaoMax,@ControleMax,'Ativo')
						END
					END
				END
					INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_COLETA_PRESSAO 
						(IdLocalColeta,DtColeta,IdSensores,Valor,UnidMedida,IdCadParametroSistema,ControleMin,EspecificacaoMin,EspecificacaoMax,ControleMax) 
						VALUES (@IdLocalMedicao,GETDATE(),@IdSensor,@Pre,@UnidMedida,@IdCadParametroSistema,@ControleMin,@EspecificacaoMin,@EspecificacaoMax,@ControleMax)
			
		END TRY

		BEGIN CATCH
			SET @ERRO = ERROR_PROCEDURE() + ': Line(' + CONVERT(varchar,ERROR_LINE()) + ') # ' + ERROR_MESSAGE()
			INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_PRESSAO_SALA_PINTURA]')
			PRINT(@ERRO)
		END CATCH

		FIM:
			PRINT('FIM')
END
GO
/****** Object:  StoredProcedure [dbo].[SPI_SP_COLETA_TEMPERATURA_SALA_PINTURA]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		THAIS LIMA DIAS
-- Create date: 13/12/2018
-- Description:	Procedure que é chamada pelo SPI_Data_Gathering Service para coletar temperatura.
-- =============================================
CREATE PROCEDURE [dbo].[SPI_SP_COLETA_TEMPERATURA_SALA_PINTURA]
	  @Ip AS VARCHAR(20),
	  @Chanel AS SMALLINT,
	  @Temp AS VARCHAR(20),
	  @UnidMedida AS VARCHAR(15)
AS

BEGIN
	--DECLARAÇÃO DE VARIAVEIS DE APOIO
		DECLARE @ERRO AS NVARCHAR(MAX)
		DECLARE @IdSensor AS BIGINT = 0
		DECLARE @IdLocalMedicao AS BIGINT = 0
		DECLARE @DescLocalMed AS VARCHAR(80)
		DECLARE @Etapa AS VARCHAR(30)	
		DECLARE @IdAlarme AS BIGINT = 0
		DECLARE @DtInicio AS DATETIME
		DECLARE @DtFim AS DATETIME
		DECLARE @Msg AS NVARCHAR(max)
		DECLARE @Sts AS NVARCHAR(50)
		DECLARE @DtReconhecimento AS DATETIME
		DECLARE @DtJustificativa AS DATETIME
		DECLARE @DtWarning AS DATETIME

		------------para inserir alarme
		DECLARE @InsertAlarme AS BIT = 0
		DECLARE @IdCadParametroSistema AS BIGINT
		DECLARE @ControleMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMax AS DECIMAL (18,2)
		DECLARE @ControleMax AS DECIMAL (18,2)

		---Variáveis que pegam os parametros para usar nas comparações
		---Pintura
		DECLARE @PIdCadParametroSistema AS BIGINT
		DECLARE @PDescParametro AS VARCHAR(30)
		DECLARE @PControleMin AS DECIMAL(18,2)
		DECLARE @PEspecificacaoMin AS DECIMAL(18,2)
		DECLARE @PEspecificacaoMax AS DECIMAL(18,2)
		DECLARE @PControleMax AS DECIMAL(18,2)
		DECLARE @PTempoAlarmeMinutos AS SMALLINT
		DECLARE @PMsgTempoAlarme AS VARCHAR(MAX)
		DECLARE @PMsgAlarme AS VARCHAR(MAX)
		---------------Cura
		DECLARE @CIdCadParametroSistema AS BIGINT
		DECLARE @CDescParametro AS VARCHAR(30)
		DECLARE @CControleMin AS DECIMAL(18,2)
		DECLARE @CEspecificacaoMin AS DECIMAL(18,2)
		DECLARE @CEspecificacaoMax AS DECIMAL(18,2)
		DECLARE @CControleMax AS DECIMAL(18,2)
		DECLARE @CTempoAlarmeMinutos AS SMALLINT
		DECLARE @CMsgTempoAlarme AS VARCHAR(MAX)
		DECLARE @CMsgAlarme AS VARCHAR(MAX) 

		BEGIN TRY
			--DEFINE O ID DE SENSOR E LOCAL DE MEDIÇÃO PARA INSERIR A MEDIÇÃO DE TEMPERATURA
				SELECT @IdSensor = IdSensores, @IdLocalMedicao = IdLocalMedicao 
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_SENSORES WHERE Ip = @Ip AND Canal=@Chanel
		
			--DEFINE A DESC DO LOCAL DE MEDIÇÃO
				SELECT @DescLocalMed = DescLocalMedicao
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_LOCAL_MEDICAO WHERE @IdLocalMedicao = IdLocalMedicao 

				IF @IdSensor=0 BEGIN
					-- SETANDO ERRO DE NÃO EXISTE SENSOR CADASTRADO PARA ESTE IP E CANAL RELATADO
						SET @ERRO = 'Não existe sensor Cadastrado para Este IP: ' + @Ip + ' Canal: ' + @Chanel
						INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_TEMPERATURA_SALA_PINTURA]')
						GOTO FIM				
				END	

				-- Parametros de Pintura 
				SELECT @PIdCadParametroSistema=IdCadParametroSistema,@PDescParametro=DescParametro,@PControleMin=ControleMin,
				@PEspecificacaoMin=EspecificacaoMin,@PEspecificacaoMax=EspecificacaoMax,@PControleMax=ControleMax,
				@PTempoAlarmeMinutos=TempoAlarmeMinutos,@PMsgTempoAlarme=MsgTempoAlarme,@PMsgAlarme=MsgAlarme
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_CADASTRO_PARAMETROS] 
				WHERE IdLocalMedicao = @IdLocalMedicao and DescParametro ='PINTURA F65 - Pintura'

				-- Parametros de Cura 
				SELECT @CIdCadParametroSistema=IdCadParametroSistema,@CDescParametro=DescParametro,@CControleMin=ControleMin,
				@CEspecificacaoMin=EspecificacaoMin,@CEspecificacaoMax=EspecificacaoMax,@CControleMax=ControleMax,
				@CTempoAlarmeMinutos=TempoAlarmeMinutos,@CMsgTempoAlarme=MsgTempoAlarme,@CMsgAlarme=MsgAlarme
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_CADASTRO_PARAMETROS] 
				WHERE IdLocalMedicao = @IdLocalMedicao and DescParametro ='PINTURA F65 - Cura'

				-- CHECAGEM DE VALORES PARA CRIAÇÃO OU FINALIZAÇÃO DE ALARMES JÁ EXISTENTES				
				-- VERIFICA SE HÁ ALARMES EXISTENTES
				SELECT @IdAlarme=IdAlarme, @DtInicio=DtInicio, @DtFim=DtFim , @Msg =Mensagem,@Sts=StatusAlarme, 
						@DtJustificativa=DtJustificativa,@DtReconhecimento=DtReconhecimento
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
				WHERE IdLocalMedicao = @IdLocalMedicao AND TipoAlarme='Temperatura' AND StatusAlarme='Ativo'

				--VERIFICA SE HÁ WARNING ABERTO
				SELECT @DtWarning = DtInicio
					FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_TEMP] 
					WHERE IdSensores = @IdSensor AND DtFim IS NULL

				
				--CONDIÇÕES PARA CASOS AONDE EXISTE UM ALARME
				IF @IdAlarme<>0 BEGIN
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTÁ NO CONTROLE MINIMO QUE REFERE-SE SOMENTE A COLETA, A CABINE FICA COM STATUS TEMPERATURA AMBIENTE
					IF @Temp < @PControleMin BEGIN
						SET @Etapa = 'Temperatura Ambiente'
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
								IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()
						SET @IdCadParametroSistema = @PIdCadParametroSistema
						SET @ControleMin=@PControleMin
						SET @EspecificacaoMin=@PEspecificacaoMin
						SET @EspecificacaoMax=@PEspecificacaoMax
						SET @ControleMax=@PControleMax	
					END
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ABAIXO DO ESPECIFICADO POR MAIS TEMPO QUE O PERMITIDO
					ELSE IF @Temp >= @PControleMin AND @Temp < @PEspecificacaoMin AND (DATEDIFF(MINUTE,GETDATE(),@DtInicio))>@PTempoAlarmeMinutos BEGIN
						SET @Etapa = SUBSTRING(@PDescParametro,CHARINDEX('-', @PDescParametro, 9)+2,LEN(@PDescParametro))	
						SET @Msg=@PMsgTempoAlarme	
						SET @IdCadParametroSistema = @PIdCadParametroSistema
						SET @ControleMin=@PControleMin
						SET @EspecificacaoMin=@PEspecificacaoMin
						SET @EspecificacaoMax=@PEspecificacaoMax
						SET @ControleMax=@PControleMax								
					END 
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ABAIXO DO ESPECIFICADO POR MENOS TEMPO QUE O PERMITIDO
					ELSE IF @Temp >= @PControleMin AND @Temp < @PEspecificacaoMin AND (DATEDIFF(MINUTE,GETDATE(),@DtInicio))<@PTempoAlarmeMinutos BEGIN
						SET @Etapa = 'Rampa 1' 
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()
						SET @IdCadParametroSistema = @PIdCadParametroSistema
						SET @ControleMin=@PControleMin
						SET @EspecificacaoMin=@PEspecificacaoMin
						SET @EspecificacaoMax=@PEspecificacaoMax
						SET @ControleMax=@PControleMax	
					END		
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR DENTRO DO ESPECIFICADO PARA PINTURA
					ELSE IF @Temp >= @PControleMin AND @Temp <= @PControleMax BEGIN
						SET @Etapa = SUBSTRING(@PDescParametro,CHARINDEX('-', @PDescParametro, 9)+2,LEN(@PDescParametro))								
						--ENCERRA O ALARME ABERTO DE ACORDO COM O QUE FOI PREENCHIDO PELO OOPERADOR NOS CAMPOS DE INTERAÇÃO
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()
						SET @IdCadParametroSistema = @PIdCadParametroSistema
						SET @ControleMin=@PControleMin
						SET @EspecificacaoMin=@PEspecificacaoMin
						SET @EspecificacaoMax=@PEspecificacaoMax
						SET @ControleMax=@PControleMax	
						
						UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_TEMP] SET DtFim=GETDATE() 
						WHERE IdSensores = @IdSensor AND DtFim IS NULL		
					END										
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ACIMA DO ESPECIFICADO DA CURA DENTRO DO TEMPO PERMITIDO
					ELSE IF @Temp > @PControleMax AND @Temp < @CControleMin AND (DATEDIFF(MINUTE,GETDATE(),@DtInicio))<@CTempoAlarmeMinutos BEGIN
						SET @Etapa = 'Rampa 2' 
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()	
						SET @IdCadParametroSistema = @PIdCadParametroSistema
						SET @ControleMin=@PControleMin
						SET @EspecificacaoMin=@PEspecificacaoMin
						SET @EspecificacaoMax=@PEspecificacaoMax
						SET @ControleMax=@PControleMax	
					END		
					
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ACIMA DO ESPECIFICADO DA CURA FORA DO TEMPO PERMITIDO
					ELSE IF @Temp > @PControleMax AND @Temp < @CControleMin AND (DATEDIFF(MINUTE,GETDATE(),@DtInicio))>@CTempoAlarmeMinutos BEGIN
						SET @Etapa = SUBSTRING(@CDescParametro,CHARINDEX('-', @CDescParametro, 9)+2,LEN(@CDescParametro))	
						SET @Msg=@PMsgTempoAlarme	
						SET @IdCadParametroSistema = @PIdCadParametroSistema
						SET @ControleMin=@PControleMin
						SET @EspecificacaoMin=@PEspecificacaoMin
						SET @EspecificacaoMax=@PEspecificacaoMax
						SET @ControleMax=@PControleMax					
					END			
							
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR DENTRO  DO ESPECIFICADO PARA CURA
					ELSE IF @Temp >= @CEspecificacaoMin  AND @Temp <= @CEspecificacaoMax BEGIN
						SET @Etapa = SUBSTRING(@CDescParametro,CHARINDEX('-', @CDescParametro, 9)+2,LEN(@CDescParametro))								
						--ENCERRA O ALARME ABERTO DE ACORDO COM O QUE FOI PREENCHIDO PELO OOPERADOR NOS CAMPOS DE INTERAÇÃO
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()
						SET @IdCadParametroSistema = @CIdCadParametroSistema
						SET @ControleMin=@CControleMin
						SET @EspecificacaoMin=@CEspecificacaoMin
						SET @EspecificacaoMax=@CEspecificacaoMax
						SET @ControleMax=@CControleMax

						UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_TEMP] SET DtFim=GETDATE() 
						WHERE IdSensores = @IdSensor AND DtFim IS NULL				
					END	
					--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ACIMA  DO ESPECIFICADO PARA CURA
					ELSE IF @Temp > @CEspecificacaoMax BEGIN
						SET @Etapa = SUBSTRING(@CDescParametro,CHARINDEX('-', @CDescParametro, 9)+2,LEN(@CDescParametro))
						SET @Msg=@CMsgTempoAlarme							
						SET @IdCadParametroSistema = @CIdCadParametroSistema
						SET @ControleMin=@CControleMin
						SET @EspecificacaoMin=@CEspecificacaoMin
						SET @EspecificacaoMax=@CEspecificacaoMax
						SET @ControleMax=@CControleMax																							
					END	
						
					--ATUALIZA O ALARME EXISTENTE COM A MENSAGEM RESPECTIVA
					UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES  SET DtFim=@DtFim, StatusAlarme=@Sts, Mensagem = @Msg  WHERE IdAlarme=@IdAlarme									
										
				END
				--CONDIÇÕES PARA CASOS AONDE NÃO EXISTE  UM ALARME
				ELSE IF @IdAlarme=0 BEGIN
					IF (@DtWarning IS NULL AND @Temp >= @PControleMin) BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_TEMP] (IdSensores,Temperatura,DtInicio) VALUES ( @IdSensor,@Temp,GETDATE())
							SET @Etapa = 'Rampa 1' 
							SET @IdCadParametroSistema = @PIdCadParametroSistema
							SET @ControleMin=@PControleMin
							SET @EspecificacaoMin=@PEspecificacaoMin
							SET @EspecificacaoMax=@PEspecificacaoMax
							SET @ControleMax=@PControleMax	
					END
					ELSE BEGIN	
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTÁ NO CONTROLE MINIMO QUE REFERE-SE SOMENTE A COLETA, A CABINE FICA COM STATUS TEMPERATURA AMBIENTE
						IF @Temp < @PControleMin BEGIN
							SET @Etapa = 'Temperatura Ambiente'
							SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
							SET @DtFim = GETDATE()
							SET @IdCadParametroSistema = @PIdCadParametroSistema
							SET @ControleMin=@PControleMin
							SET @EspecificacaoMin=@PEspecificacaoMin
							SET @EspecificacaoMax=@PEspecificacaoMax
							SET @ControleMax=@PControleMax	
						END							
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ABAIXO DO ESPECIFICADO POR MAIS TEMPO QUE O PERMITIDO
						ELSE IF @Temp >= @PControleMin AND @Temp < @PEspecificacaoMin AND (DATEDIFF(MINUTE,GETDATE(),@DtWarning))>@PTempoAlarmeMinutos BEGIN
							SET @Etapa = SUBSTRING(@PDescParametro,CHARINDEX('-', @PDescParametro, 9)+2,LEN(@PDescParametro))	
							SET @Msg=@PMsgTempoAlarme	
							SET @InsertAlarme =1
							SET @IdCadParametroSistema = @PIdCadParametroSistema
							SET @ControleMin=@PControleMin
							SET @EspecificacaoMin=@PEspecificacaoMin
							SET @EspecificacaoMax=@PEspecificacaoMax
							SET @ControleMax=@PControleMax
						END 
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ABAIXO DO ESPECIFICADO POR MENOS TEMPO QUE O PERMITIDO
						ELSE IF @Temp >= @PControleMin AND @Temp < @PEspecificacaoMin AND (DATEDIFF(MINUTE,GETDATE(),@DtWarning))<@PTempoAlarmeMinutos BEGIN
							SET @Etapa = 'Rampa 1' 
							SET @IdCadParametroSistema = @PIdCadParametroSistema
							SET @ControleMin=@PControleMin
							SET @EspecificacaoMin=@PEspecificacaoMin
							SET @EspecificacaoMax=@PEspecificacaoMax
							SET @ControleMax=@PControleMax	

						END		
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR DENTRO DO ESPECIFICADO PARA PINTURA
						ELSE IF @Temp >= @PControleMin AND @Temp <= @PControleMax BEGIN
							SET @Etapa = SUBSTRING(@PDescParametro,CHARINDEX('-', @PDescParametro, 9)+2,LEN(@PDescParametro))		
							SET @IdCadParametroSistema = @PIdCadParametroSistema
							SET @ControleMin=@PControleMin
							SET @EspecificacaoMin=@PEspecificacaoMin
							SET @EspecificacaoMax=@PEspecificacaoMax
							SET @ControleMax=@PControleMax	

							UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_TEMP] SET DtFim=GETDATE() 
							WHERE IdSensores = @IdSensor AND DtFim = NULL		
						END										
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ACIMA DO ESPECIFICADO DA CURA DENTRO DO TEMPO PERMITIDO
						ELSE IF @Temp > @PControleMax AND @Temp < @CControleMin AND (DATEDIFF(MINUTE,GETDATE(),@DtWarning))<@CTempoAlarmeMinutos BEGIN
							SET @Etapa = 'Rampa 2' 	
							SET @IdCadParametroSistema = @PIdCadParametroSistema
							SET @ControleMin=@PControleMin
							SET @EspecificacaoMin=@PEspecificacaoMin
							SET @EspecificacaoMax=@PEspecificacaoMax
							SET @ControleMax=@PControleMax	
						END		
					
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ACIMA DO ESPECIFICADO DA CURA FORA DO TEMPO PERMITIDO
						ELSE IF @Temp > @PControleMax AND @Temp < @CControleMin AND (DATEDIFF(MINUTE,GETDATE(),@DtWarning))>@CTempoAlarmeMinutos BEGIN
							SET @Etapa = SUBSTRING(@CDescParametro,CHARINDEX('-', @CDescParametro, 9),LEN(@CDescParametro))	
							SET @Msg=@PMsgTempoAlarme			
							SET @InsertAlarme =1		
							SET @IdCadParametroSistema = @CIdCadParametroSistema
							SET @ControleMin=@CControleMin
							SET @EspecificacaoMin=@CEspecificacaoMin
							SET @EspecificacaoMax=@CEspecificacaoMax
							SET @ControleMax=@CControleMax	
						END			
							
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR DENTRO  DO ESPECIFICADO PARA CURA
						ELSE IF @Temp >= @CEspecificacaoMin  AND @Temp <= @CEspecificacaoMax BEGIN
							SET @Etapa = SUBSTRING(@CDescParametro,CHARINDEX('-', @CDescParametro, 9)+2,LEN(@CDescParametro))								
							SET @IdCadParametroSistema = @CIdCadParametroSistema
							SET @ControleMin=@CControleMin
							SET @EspecificacaoMin=@CEspecificacaoMin
							SET @EspecificacaoMax=@CEspecificacaoMax
							SET @ControleMax=@CControleMax	


							UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_TEMP] SET DtFim=GETDATE() 
							WHERE IdSensores = @IdSensor AND DtFim = NULL			
						END	
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ACIMA  DO ESPECIFICADO PARA CURA
						ELSE IF @Temp > @CEspecificacaoMax BEGIN
							SET @Etapa = SUBSTRING(@CDescParametro,CHARINDEX('-', @CDescParametro, 9)+2,LEN(@CDescParametro))
							SET @Msg=@CMsgTempoAlarme	
							SET @InsertAlarme =1			
							SET @IdCadParametroSistema = @CIdCadParametroSistema
							SET @ControleMin=@CControleMin
							SET @EspecificacaoMin=@CEspecificacaoMin
							SET @EspecificacaoMax=@CEspecificacaoMax
							SET @ControleMax=@CControleMax															
						END						

						IF @InsertAlarme = 1 BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
								(IdLocalMedicao,TipoAlarme,DtInicio,Mensagem,IdCadParametroSistema,ControleMin,EspecificacaoMin,EspecificacaoMax,ControleMax,StatusAlarme) 
								VALUES (@IdLocalMedicao,'Temperatura',GETDATE(),@Msg,@IdCadParametroSistema,@ControleMin,@EspecificacaoMin,@EspecificacaoMax,@ControleMax,'Ativo')
						END
					END
				END
					INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_COLETA_TEMPERATURA 
						(IdLocalColeta,DtColeta,IdSensores,Valor,UnidMedida,IdCadParametroSistema,ControleMin,EspecificacaoMin,EspecificacaoMax,ControleMax,Etapa) 
						VALUES (@IdLocalMedicao,GETDATE(),@IdSensor,@Temp,@UnidMedida,@IdCadParametroSistema,@ControleMin,@EspecificacaoMin,@EspecificacaoMax,@ControleMax,@Etapa)
			
		END TRY

		BEGIN CATCH
			SET @ERRO = ERROR_PROCEDURE() + ': Line(' + CONVERT(varchar,ERROR_LINE()) + ') # ' + ERROR_MESSAGE()
			INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_TEMPERATURA_SALA_PINTURA]')
			PRINT(@ERRO)
		END CATCH

		FIM:
			PRINT('FIM')
END
GO
/****** Object:  StoredProcedure [dbo].[SPI_SP_COLETA_UMIDADE_SALA_PINTURA]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		THAIS LIMA DIAS
-- Create date: 18/12/2018
-- Description:	Procedure que é chamada pelo SPI_Data_Gathering Service para coletar umidade dos sensores.
-- =============================================
CREATE PROCEDURE [dbo].[SPI_SP_COLETA_UMIDADE_SALA_PINTURA]
	  @Ip AS VARCHAR(20),
	  @Chanel AS SMALLINT,
	  @Umid AS VARCHAR(20),
	  @UnidMedida AS VARCHAR(15)
AS

BEGIN
	--DECLARAÇÃO DE VARIAVEIS DE APOIO
		DECLARE @ERRO AS NVARCHAR(MAX)
		DECLARE @IdSensor AS BIGINT = 0
		DECLARE @IdLocalMedicao AS BIGINT = 0
		DECLARE @DescLocalMed AS VARCHAR(80)
		DECLARE @Etapa AS VARCHAR(30)	
		DECLARE @IdAlarme AS BIGINT = 0
		DECLARE @DtInicio AS DATETIME
		DECLARE @DtFim AS DATETIME
		DECLARE @Msg AS NVARCHAR(max)
		DECLARE @Sts AS NVARCHAR(50)
		DECLARE @DtReconhecimento AS DATETIME
		DECLARE @DtJustificativa AS DATETIME
		DECLARE @DtWarning AS DATETIME

		------------para inserir alarme E COLETA
		DECLARE @InsertAlarme AS BIT = 0
		DECLARE @IdCadParametroSistema AS BIGINT
		DECLARE @DescParametro AS VARCHAR(30)
		DECLARE @ControleMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMin AS DECIMAL (18,2)
		DECLARE @EspecificacaoMax AS DECIMAL (18,2)
		DECLARE @ControleMax AS DECIMAL (18,2)
		DECLARE @MsgTempoAlarme AS VARCHAR(MAX)
		DECLARE @MsgAlarme AS VARCHAR(MAX)
		DECLARE @TempoAlarmeMinutos AS SMALLINT

		

		BEGIN TRY
			--DEFINE O ID DE SENSOR E LOCAL DE MEDIÇÃO PARA INSERIR A MEDIÇÃO DE UMIDADE
				SELECT @IdSensor = IdSensores, @IdLocalMedicao = IdLocalMedicao 
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_SENSORES WHERE Ip = @Ip AND Canal=@Chanel
		
			--DEFINE A DESC DO LOCAL DE MEDIÇÃO
				SELECT @DescLocalMed = DescLocalMedicao
				FROM SPI_DB_EMBRAER_COLETA.dbo.TB_LOCAL_MEDICAO WHERE @IdLocalMedicao = IdLocalMedicao 

				IF @IdSensor=0 BEGIN
					-- SETANDO ERRO DE NÃO EXISTE SENSOR CADASTRADO PARA ESTE IP E CANAL RELATADO
						SET @ERRO = 'Não existe sensor Cadastrado para Este IP: ' + @Ip + ' Canal: ' + @Chanel
						INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_UMIDADE_SALA_PINTURA]')
						GOTO FIM				
				END	

				-- Parametros de Umidade
				SELECT @IdCadParametroSistema=IdCadParametroSistema,@DescParametro=DescParametro,@ControleMin=ControleMin,
				@EspecificacaoMin=EspecificacaoMin,@EspecificacaoMax=EspecificacaoMax,@ControleMax=ControleMax,
				@TempoAlarmeMinutos=TempoAlarmeMinutos,@MsgTempoAlarme=MsgTempoAlarme,@MsgAlarme=MsgAlarme
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_CADASTRO_PARAMETROS] 
				WHERE IdLocalMedicao = @IdLocalMedicao and DescParametro ='PINTURA F65 - Umidade'



				-- CHECAGEM DE VALORES PARA CRIAÇÃO OU FINALIZAÇÃO DE ALARMES JÁ EXISTENTES				
				-- VERIFICA SE HÁ ALARMES EXISTENTES
				SELECT @IdAlarme=IdAlarme, @DtInicio=DtInicio, @DtFim=DtFim , @Msg =Mensagem,@Sts=StatusAlarme, 
						@DtJustificativa=DtJustificativa,@DtReconhecimento=DtReconhecimento
				FROM [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
				WHERE IdLocalMedicao = @IdLocalMedicao AND TipoAlarme='Umidade' AND StatusAlarme='Ativo'

				--VERIFICA SE HÁ WARNING ABERTO
				SELECT @DtWarning = DtInicio
					FROM [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_UMID] 
					WHERE IdSensores = @IdSensor AND DtFim IS NULL

				
				--CONDIÇÕES PARA CASOS AONDE EXISTE UM ALARME
				IF @IdAlarme<>0 BEGIN
					--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTÁ ABAIXO DO ESPECIFICADO MINIMO POR MAIS TEMPO QUE O PERMITIDO SETANDO A MENSAGEM DE ALARME DE BAIXA UMIDADE
					IF @Umid < @EspecificacaoMin BEGIN  						
						IF (DATEDIFF(MINUTE,GETDATE(),@DtInicio))>@TempoAlarmeMinutos BEGIN
							SET @Msg=@MsgTempoAlarme
						END	
						ELSE BEGIN				
							SET @Msg=@MsgAlarme	
						END						
					END
					--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTÁ DENTRO DO ESPECIFICADO MINIMO E MÁXIMO ENCERRANDO ALARME DE BAIXA UMIDADE
					ELSE IF @Umid >= @EspecificacaoMin AND @Umid <= @EspecificacaoMax  BEGIN
						SET @Etapa = SUBSTRING(@DescParametro,CHARINDEX('-', @DescParametro, 9)+2,LEN(@DescParametro))	
						--ENCERRA O ALARME ABERTO DE ACORDO COM O QUE FOI PREENCHIDO PELO OOPERADOR NOS CAMPOS DE INTERAÇÃO
						SET @Sts = IIF(@DtReconhecimento IS NULL, 'Encerrado Sem Reconhecimento', 
									IIF(@DtJustificativa IS NULL,'Encerrado Sem Justificativa','Encerrado'))
						SET @DtFim = GETDATE()

						--ENCERRA O WARNING DE UMIDADE FORA DO ESPECIFICADO
						UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_UMID] SET DtFim=GETDATE() 
						WHERE IdSensores = @IdSensor AND DtFim IS NULL
					END
					--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTÁ ACIMA DO ESPECIFICADO MAXIMO POR MAIS TEMPO QUE O PERMITIDO SETANDO A MENSAGEM DE ALARME DE ALTA UMIDADE
					IF @Umid > @EspecificacaoMax BEGIN
						IF (DATEDIFF(MINUTE,GETDATE(),@DtInicio))>@TempoAlarmeMinutos BEGIN
							SET @Msg=@MsgAlarme	
						END	
						ELSE BEGIN				
							SET @Msg=@MsgTempoAlarme
						END							
					END
						
					--ATUALIZA O ALARME EXISTENTE COM O STATUS E MENSAGEM RESPECTIVA
					UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES  SET DtFim=@DtFim, StatusAlarme=@Sts, Mensagem = @Msg  WHERE IdAlarme=@IdAlarme													
				END
				--CONDIÇÕES PARA CASOS AONDE NÃO EXISTE  UM ALARME
				ELSE IF @IdAlarme=0 BEGIN
					IF (@DtWarning IS NULL AND @Umid < @EspecificacaoMin) BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_UMID] (IdSensores,Umidade,DtInicio) VALUES ( @IdSensor,@Umid,GETDATE())
					END
					ELSE BEGIN						
						--CONDIÇÃO QUE CONFERE SE A TEMPERATURA ESTA COM O VALOR ABAIXO DO ESPECIFICADO POR MAIS TEMPO QUE O PERMITIDO
						IF @Umid < @EspecificacaoMin BEGIN		
							IF (DATEDIFF(MINUTE,GETDATE(),@DtWarning))>@TempoAlarmeMinutos BEGIN
								SET @Msg=@MsgAlarme	
								SET @InsertAlarme = 1	
							END	
							ELSE BEGIN				
								SET @Msg=@MsgTempoAlarme
							END	
							
						END 		
						--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTA COM O VALOR DENTRO  DO ESPECIFICADO 
						ELSE IF @Umid >= @EspecificacaoMin  AND @Umid <= @EspecificacaoMax BEGIN										
							--ENCERRA O WARNING QUE ESTÁ ABERTO
							UPDATE [SPI_DB_EMBRAER_COLETA].[dbo].[TB_WARNING_UMID] SET DtFim=GETDATE() 
							WHERE IdSensores = @IdSensor AND DtFim IS NULL			
						END	
						--CONDIÇÃO QUE CONFERE SE A UMIDADE ESTA COM O VALOR ACIMA DO ESPECIFICADO POR MAIS TEMPO QUE O PERMITIDO
						ELSE IF @Umid > @EspecificacaoMax BEGIN		
							IF (DATEDIFF(MINUTE,GETDATE(),@DtWarning))>@TempoAlarmeMinutos BEGIN
							SET @Msg=@MsgAlarme	
						END	
						ELSE BEGIN				
							SET @Msg=@MsgTempoAlarme
						END	
							SET @InsertAlarme = 1
						END 					

						IF @InsertAlarme = 1 BEGIN
							INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_ALARMES 
								(IdLocalMedicao,TipoAlarme,DtInicio,Mensagem,IdCadParametroSistema,ControleMin,EspecificacaoMin,EspecificacaoMax,ControleMax,StatusAlarme) 
								VALUES (@IdLocalMedicao,'Umidade',GETDATE(),@Msg,@IdCadParametroSistema,@ControleMin,@EspecificacaoMin,@EspecificacaoMax,@ControleMax,'Ativo')
						END
					END
				END
					INSERT INTO [SPI_DB_EMBRAER_COLETA].[dbo].TB_COLETA_UMIDADE 
						(IdLocalColeta,DtColeta,IdSensores,Valor,UnidMedida,IdCadParametroSistema,ControleMin,EspecificacaoMin,EspecificacaoMax,ControleMax) 
						VALUES (@IdLocalMedicao,GETDATE(),@IdSensor,@Umid,@UnidMedida,@IdCadParametroSistema,@ControleMin,@EspecificacaoMin,@EspecificacaoMax,@ControleMax)
			
		END TRY

		BEGIN CATCH
			SET @ERRO = ERROR_PROCEDURE() + ': Line(' + CONVERT(varchar,ERROR_LINE()) + ') # ' + ERROR_MESSAGE()
			INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_COLETA_UMIDADE_SALA_PINTURA]')
			PRINT(@ERRO)
		END CATCH

		FIM:
			PRINT('FIM')
END
GO
/****** Object:  StoredProcedure [dbo].[SPI_SP_DG_MIGRATION]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		THAIS LIMA DIAS
-- Create date: 12/10/2018
-- Description:	Procedure que é chamada pelo SPI_Data_Gathering Service
-- =============================================
CREATE PROCEDURE [dbo].[SPI_SP_DG_MIGRATION]
	  @Ip AS VARCHAR(20),
	  @Ch AS VARCHAR(50),
      @En AS VARCHAR(50),
      @Rng AS VARCHAR(50),
      @Val AS VARCHAR(50),
      @EgF AS VARCHAR(50),
      @Evt AS VARCHAR(50),
      @LoA AS VARCHAR(50),
      @HiA AS VARCHAR(50),
      @HVal AS VARCHAR(50),
      @HEgF AS VARCHAR(50),
      @LVal AS VARCHAR(50),
      @LEgF AS VARCHAR(50),
      @SVal AS VARCHAR(50),
      @ClrH AS VARCHAR(50),
      @ClrL AS VARCHAR(50),
      @PEgF AS VARCHAR(50),
      @Uni AS VARCHAR(50),
	  @Md AS VARCHAR(50),
      @Stat AS VARCHAR(50),
      @Cnting AS VARCHAR(50),
      @OvLch AS VARCHAR(50)--,
	--@OUT_STATUS AS VARCHAR(10) output
AS
BEGIN
	DECLARE @NOW AS DATETIME = GETDATE()
	DECLARE @ERRO AS VARCHAR(1000)

	--DECLARAÇÃO DE VARIAVEIS DE APOIO
	DECLARE @ID_TRANS BIGINT 
	DECLARE @ID_TMP_ARG BIGINT
	DECLARE @ID_TAG_TMP_ARG BIGINT
	DECLARE @OPCADDRESS_TMP_ARG  VARCHAR(100)
	DECLARE @VLFIXO_TMP_ARG VARCHAR(100)
	DECLARE @CAMPO_OPCADDRESS VARCHAR (100)
	DECLARE @sSql VARCHAR (255)
	DECLARE @ID_TBCP_OPC BIGINT
	DECLARE @VL_TBCP_OPC NVARCHAR (max) 

	BEGIN TRY

		-- Busca se já existe essa TAG registrada no banco de dados
		DECLARE @VERIFICA_ID AS BIGINT
		DECLARE @Id AS BIGINT
		SELECT @VERIFICA_ID = Ch FROM SPI_TB_DG_CHANEL WHERE Ch = @Ch And Ip=@Ip


		-- Atualiza a TAG no banco de dados
		IF @VERIFICA_ID IS NULL BEGIN
			INSERT INTO SPI_TB_DG_CHANEL (Ip,Ch,En,Rng, Val,EgF,Evt,LoA,HiA,HVal,HEgF,LVal,LEgF,SVal,ClrH,ClrL,PEgF,Uni,Md,Stat,Cnting,OvLch ,Changed_At)
			VALUES (@Ip,@Ch,@En,@Rng, @Val,@EgF,@Evt,@LoA,@HiA,@HVal,@HEgF,@LVal,@LEgF,@SVal,@ClrH,@ClrL,@PEgF,@Uni,@Md,@Stat,@Cnting,@OvLch,@NOW)			
		END ELSE BEGIN
			UPDATE SPI_TB_DG_CHANEL
			SET Ip=@Ip,Ch=@Ch,En=@En,Rng=@Rng, Val=@Val,EgF=@EgF,Evt=@Evt,LoA=@LoA,HiA=@HiA,HVal=@HVal,HEgF=@HEgF,LVal=@LVal,LEgF=@LEgF,SVal=@SVal,ClrH=@ClrH,ClrL=@ClrL,PEgF=@PEgF,Uni=@Uni, Md=@Md,Stat=@Stat,Cnting=@Cnting,OvLch=@OvLch,Changed_At = @NOW
			WHERE Ch = @Ch And Ip=@Ip
		END

		--Select ID da Tag para Pricurar transações
		SELECT @Id = Id FROM SPI_TB_DG_CHANEL WHERE Ch = @Ch And Ip=@Ip

		-- Busca tansações no banco para a tag
		DECLARE @PROCEDURE AS NVARCHAR(max)

		-- Cursor para percorrer os nomes dos objetos 
		DECLARE CURSOR_TRANSACAO CURSOR FOR
		SELECT ProcedureName,ID  FROM SPI_TB_DG_TRANSACTIONS WHERE TriggerTagId = @Id and Enable = 'true' and (TriggerValue = @Val or TriggerValue is null)


	-- Abrindo Cursor para leitura
	OPEN CURSOR_TRANSACAO


	-- Lendo a próxima linha
	FETCH NEXT FROM CURSOR_TRANSACAO INTO @PROCEDURE,@ID_TRANS



	-- Percorrendo linhas do cursor (enquanto houverem)
	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- LIMPANDO VARICAVEIS AUXILIARES
			SET @ID_TMP_ARG =NULL
			SET @ID_TAG_TMP_ARG =NULL
			SET @OPCADDRESS_TMP_ARG  =NULL
			SET @VLFIXO_TMP_ARG = NULL
			SET @CAMPO_OPCADDRESS = NULL

		--TABELA TEMPORARIA COM TODOS OS ARGUMENTOS DE PROCEDURE
		CREATE TABLE #TEMP_ARGUMENTS
		(
			ID bigint ,
			ID_TRANS bigint ,
			POS int ,
			ID_TAG bigint ,
			CAMPO_OPCADDRESS nvarchar (50) ,
			VL_FIXO nvarchar (200)
		)

		INSERT INTO #TEMP_ARGUMENTS
			SELECT * FROM SPI_TB_DG_ARGUMENTS WHERE ID_TRANS=@ID_TRANS ORDER BY POS 

		WHILE (SELECT COUNT(*) FROM #TEMP_ARGUMENTS) > 0 -- WHILE PARA PERCORRER OS REGISTROS
		BEGIN
			--PEGANDO PRIMEIRO ARGUMENTO
			SELECT TOP 1 @ID_TMP_ARG = T.ID,@ID_TAG_TMP_ARG = T.ID_TAG,@OPCADDRESS_TMP_ARG = T.CAMPO_OPCADDRESS,@VLFIXO_TMP_ARG=VL_FIXO  FROM  #TEMP_ARGUMENTS T
			IF (@ID_TAG_TMP_ARG IS NOT NULL AND @OPCADDRESS_TMP_ARG IS NOT NULL)
			BEGIN
				SET  @sSql = NULL
				SET @ID_TBCP_OPC= NULL 
				SET @CAMPO_OPCADDRESS = NULL 
			
				--cRIANDO TABELA TEMPORARIA CAMPO OPCADRESS
				CREATE TABLE #CAMPO_OPCADDRESS
				(
					ID bigint ,
					VL_CP_OPC nvarchar (max) 
				)

				SET  @sSql = 'INSERT INTO #CAMPO_OPCADDRESS (ID, VL_CP_OPC) SELECT Id, CONVERT(NVARCHAR(max), ' + @OPCADDRESS_TMP_ARG + ') FROM SPI_TB_DG_CHANEL WHERE Id = '+ CONVERT(NVARCHAR,@ID_TAG_TMP_ARG) 
				EXEC(@sSql)

				---- PEGA O VL DO  CAMPO DA IL_ADDRESS E JOGA COMO PARAMETRO DA PROCEDURE

				SELECT TOP 1 @ID_TBCP_OPC = T.ID,@CAMPO_OPCADDRESS = T.VL_CP_OPC FROM  #CAMPO_OPCADDRESS T
								
				SET @PROCEDURE = @PROCEDURE  +'''' + @CAMPO_OPCADDRESS  +''''

				IF ((SELECT COUNT(*) FROM #TEMP_ARGUMENTS) >  1)
				BEGIN
					SET @PROCEDURE = @PROCEDURE + ',' 
				END

				DELETE FROM #CAMPO_OPCADDRESS WHERE ID= @ID_TBCP_OPC
				DROP TABLE  #CAMPO_OPCADDRESS
			END

			IF (@VLFIXO_TMP_ARG IS NOT NULL)
			BEGIN
				SET @PROCEDURE = @PROCEDURE +'''' + @VLFIXO_TMP_ARG +''''

				IF ((SELECT COUNT(*) FROM #TEMP_ARGUMENTS) >  1)
				BEGIN
					SET @PROCEDURE = @PROCEDURE + ',' 
				END

			END

			DELETE FROM #TEMP_ARGUMENTS WHERE ID= @ID_TMP_ARG
		END

		EXECUTE (@PROCEDURE)
		-- Lendo a próxima linha
		FETCH NEXT FROM CURSOR_TRANSACAO INTO @PROCEDURE,@ID_TRANS

		--DROP TABLE TEMP_ARGUMENTS
		DROP TABLE  #TEMP_ARGUMENTS

	END

	-- Fechando Cursor para leitura
	CLOSE CURSOR_TRANSACAO

	-- Desalocando o cursor
	DEALLOCATE CURSOR_TRANSACAO

	--SET @OUT_STATUS = 'OK'

	
	END TRY
	BEGIN CATCH


	SET @ERRO = ERROR_PROCEDURE() + ': Line(' + CONVERT(varchar,ERROR_LINE()) + ') # ' + ERROR_MESSAGE()
			INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_DG_MIGRATION]')
			--SET @OUT_STATUS ='NOK'
	END CATCH
END

GO
/****** Object:  StoredProcedure [dbo].[SPI_SP_TRATA_DADOS]    Script Date: 03/01/2019 15:18:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		THAIS LIMA DIAS
-- Create date: 12/10/2018
-- Description:	Procedure que é chamada pelo SPI_Data_Gathering Service
-- =============================================

CREATE  PROCEDURE [dbo].[SPI_SP_TRATA_DADOS]
		@Temperatura as VARCHAR(20)--,
		--@OUT_STATUS AS VARCHAR(10) output
AS
BEGIN
	
	DECLARE @ERRO AS VARCHAR(1000)
	DECLARE @NOW AS DATETIME = GETDATE()

	BEGIN TRY
			
			insert into SPI_TESTE.dbo.TB_TESTE(TES,Data) values (@Temperatura,getdate())

			--SET @OUT_STATUS = 'OK'			   

	END TRY


	BEGIN CATCH


		SET @ERRO = ERROR_PROCEDURE() + ': Line(' + CONVERT(varchar,ERROR_LINE()) + ') # ' + ERROR_MESSAGE()
				INSERT INTO [dbo].[SPI_TB_DG_ERROR] ([Date],[Mensagem],[ProcedureName]) VALUES (GETDATE(),@ERRO,'[SPI_SP_TRATA_DADOS]')
				--SET @OUT_STATUS ='NOK'
	END CATCH
END
GO
USE [master]
GO
ALTER DATABASE [SPI_DATA_GATHERING] SET  READ_WRITE 
GO
