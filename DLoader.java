/*
 * Copyright (C) 2017 Kelvin Wong
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import com.gh.Bku;
import com.gh.Zip;
import com.gh.Ini;
import com.gh.Pdf;
import com.gh.Log;

import com.itextpdf.text.DocumentException;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * DLoader Program (based on DMS Loader Version 2.3.4)
 * 
 * @author Kelvin Wong
 * @version 1.0
 * 
 */
public final class DLoader {
    final public static String PROGRAMVERSION = "DLoader Program Version 1.0";
    final public static String COPYRIGHT = "Copyright 2017 Kelvin Wong";
    final public static String USAGE = "Usage : DLoader";
    public static void main(String[] args){
        File fini = new File("DLoader.ini");
        if(args.length!=0){ 
            System.out.println(PROGRAMVERSION); 
            System.out.println(COPYRIGHT); 
            System.out.println(USAGE); 
        }
        else if(fini.isDirectory()){ System.out.println(getPathName(fini)+" is folder!"); }
        else{
            DLoader obj = new DLoader(fini);
            int n = obj.scan();
            if(n>0){ System.out.println("Total "+n+" File(s) loaded"); }
            else{ System.out.println("No file loaded"); }
        }
    }
    final String[][] DEFAULTPROPERTIES = {
        {"InputPath", ".\\In"},
        {"OutputPath", ".\\Out"},
        {"ErrorPath", ".\\Err\\{YYYYMMDD}"},
        {"DmsLstFileExt","dmslst"},
        {"Zip", "true"},
        {"SplitZip", "true"},
        {"MaxZipEntry", "3500"},
        {"MaxZipSize", "1000000000"},
        {"BackupPath", ".\\Backup\\{YYYYMMDD}"},
        {"LogFile",".\\Log\\{YYYYMMDD}.log"},
        {"AuditFile",""},
        {"LogDisplay", "true"},
        {"Verbose", "true"},
        {"PdfEncrypt","false"},
    };
   
    boolean bZip;
    boolean bSplitZip;
    int nMaxZipEntry;
    long lMaxZipSize;
    String dmsLstFileExt;
    boolean bPdfEncrypt;
    
    String inputPath;
    String outputPath;
    String errorPath;

    DLoader(File fini){ init(ini.readIni(fini,DEFAULTPROPERTIES)); }
    DLoader(Properties cfg){ init(cfg); }
    void init(Properties cfg){

        String inputpath = cfg.getProperty("InputPath");
        String outputpath = cfg.getProperty("OutputPath");
        String errorpath = cfg.getProperty("ErrorPath");
        String dmslstfileext = cfg.getProperty("DmsLstFileExt");
        boolean bzip = Boolean.parseBoolean(cfg.getProperty("Zip"));
        boolean bsplitzip = Boolean.parseBoolean(cfg.getProperty("SplitZip")); 
        int nmaxzipentry = Integer.parseInt(cfg.getProperty("MaxZipEntry")); 
        long lmaxzipsize = Long.parseLong(cfg.getProperty("MaxZipSize")); 

        String auditfile = cfg.getProperty("AuditFile");
        String backuppath = cfg.getProperty("BackupPath");
        String logfile = cfg.getProperty("LogFile");
        boolean blogdisplay = Boolean.parseBoolean(cfg.getProperty("LogDisplay"));
        boolean bverbose = Boolean.parseBoolean(cfg.getProperty("Verbose"));
        boolean bpdfencrypt = Boolean.parseBoolean(cfg.getProperty("PdfEncrypt"));
        initEps(backuppath,logfile,auditfile,blogdisplay,bverbose);
        initDLoader(inputpath,outputpath,errorpath,dmslstfileext,bzip,bsplitzip,nmaxzipentry,lmaxzipsize,bpdfencrypt);
    }
    void initDLoader(String inputPath,String outputPath,String errorPath,String DmsLstFileExt,boolean bZip,boolean bSplitZip,int nMaxZipEntry,long lMaxZipSize,boolean bPdfEncrypt){
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.errorPath = errorPath;
        this.dmsLstFileExt = DmsLstFileExt;
        this.bZip = bZip;
        this.bSplitZip = bSplitZip;
        this.nMaxZipEntry = nMaxZipEntry;
        this.lMaxZipSize = lMaxZipSize;
        this.bPdfEncrypt = bPdfEncrypt;
    }
    public int scan(){
        int nList = 0;
        File inFolder = getDynamicInFolder(inputPath);
        if(inFolder!=null){
            List<File> outputFileList = new ArrayList<>();
            if(bVerbose){ System.out.println("Scan "+getPathName(inFolder)); }
            for(File file: inFolder.listFiles()){
                if(file.isFile() && file.length()>0 && file.getName().toUpperCase().endsWith("."+dmsLstFileExt.toUpperCase())){
                    if(bVerbose){ System.out.println("List File : "+getPathName(file)+" found"); }
                    File outFolder = null;
                    List<File> dmsFileList = readExtFileList(file,inFolder,".dms");
                    if(dmsFileList.isEmpty()){ log.write("WARN",getPathName(file)+" is found an empty list"); }
                    else{ 
                        if(bVerbose){ for(File dmsFile:dmsFileList){ System.out.println(getPathName(dmsFile)); } }
                        log.write("Load " + getPathName(file) + " " + dmsFileList.size()+ " File(s)"); 
                        log.audit(file.getName()+",DMSLST,1,R");
                        outFolder = getDynamicOutFolder(outputPath,file);
                    }
                    if(outFolder!=null){
                        List<String> csvLines = new ArrayList<>();
                        String outName = getFileName(file.getName());
                        String zipName = (bZip) ? outName+".zip" : null;
                        if(bZip){ zip.open(); }
                        int nZipSerial;
                        List<File> outFileList = new ArrayList<>();
                        for(File dmsFile : dmsFileList){
                            log.write("Load DMS file : "+getPathName(dmsFile));
                            log.audit(dmsFile.getName()+",DMS,1,R");
                            nZipSerial = (outFileList.isEmpty()) ? 1 : outFileList.size();
                            List<File> fileList;
                            if(!bZip){ fileList = loadDms(dmsFile,csvLines,outFolder); }
                            else if(!bSplitZip){ fileList = loadDms(dmsFile,csvLines,outFolder,zipName); }
                            else { fileList = loadDms(dmsFile,csvLines,outFolder,zipName,nMaxZipEntry,lMaxZipSize,nZipSerial); }
                            outFileList.addAll(fileList); 
                            if(bBackup){ 
                                if(bku.backupFile(dmsFile)==false){ log.write("WARN","Failed to backup "+getPathName(file)); }
                            }
                            else if(dmsFile.delete()){ log.write("Delete "+getPathName(dmsFile)); }
                            else{ log.write("WARN","Failed to delete "+getPathName(dmsFile)); }
                        }
                        if(csvLines.size()>0){
                            String docType = getDocType(csvLines.get(0));
                            String s; byte[] buf;
                            switch(docType){
                                case "PF" : s="PF_E"; buf = formatCsv(pfCsvFieldTitle,new ByteArrayOutputStream(),csvLines); break;
                                case "PT" : s="PT_K"; buf = formatCsv(ptCsvFieldTitle,new ByteArrayOutputStream(),csvLines); break;
                                case "CTR" : s="CTR"; buf = formatCsv(ctCsvFieldTitle,new ByteArrayOutputStream(),csvLines); break;
                                default : s=""; buf = formatCsv(null,new ByteArrayOutputStream(),csvLines); break;
                            }
                            String csvName;
                            switch(docType){
                            		case "PF" : csvName = "IXX_B_000"+s+"_"+new SimpleDateFormat("yyyyMMddHHmm_0SSS").format(Calendar.getInstance().getTime()); break;
                                default   : csvName = "IXX_B_132"+s+"_"+new SimpleDateFormat("yyyyMMddHHmm_0SSS").format(Calendar.getInstance().getTime()); break;
                            }
                            File fcsv = new File(outFolder,csvName+".csv");
                            log.write("Output Csv File : "+fcsv.getName()+" "+csvLines.size()+" Entry(s)"); 
                            if(bZip){ zip.write(fcsv.getName(),buf); }
                            else{ outputFile(fcsv,buf); }
                            if(!outFileList.isEmpty()){ outputFileList.addAll(outFileList); }
                            log.audit(csvName+",CSV,1,Z"); nList++;
                            if(bZip){ log.audit(outName+",ZIP,"+outFileList.size()+",W"); }
                        }
                        if(bZip){ zip.close(); }
                        if(bBackup){ 
                            if(bku.backupFile(file)==false){ log.write("WARN","Failed to backup "+getPathName(file)); }
                        }
                        else if(file.delete()){ log.write("Delete "+getPathName(file)); }
                        else{ log.write("WARN","Failed to delete "+getPathName(file)); }
                    }
                }
            }
            if(outputFileList.size()>0){ 
                for(File file : outputFileList){ 
                    System.out.println(getPathName(file)); 
                }
                log.write("Total "+outputFileList.size()+" File(s) generated"); 
            }
        }
        if(nList>0){ log.write("Total "+nList+" List File(s) processed"); }
        log.outputFile();
        return(nList);
    }
    List<File> loadDms(File fin,List<String> csvLines,File outFolder){
        return(loadDms(fin,csvLines,outFolder,null,false,0,0,false,-1));
    }
    List<File> loadDms(File fin,List<String> csvLines,File outFolder,String zipName){
        return(loadDms(fin,csvLines,outFolder,zipName,true,0,0,false,-1));
    }
    List<File> loadDms(File fin,List<String> csvLines,File outFolder,String zipName,int nMaxZipEntry,long lMaxZipSize,int nZipSerial){
        return(loadDms(fin,csvLines,outFolder,zipName,true,nMaxZipEntry,lMaxZipSize,true,nZipSerial));
    }
    List<File> loadDms(File fin,List<String> csvLines,File outFolder,String zipName,boolean bZip,int nMaxZipEntry,long lMaxZipSize,boolean bSplitZip,int nZipSerial){
        List<File> outputFileList = new ArrayList<>();
        int nInPdf=0,nOutPdf=0;
        log.write("Read "+fin.getName() + " "+fin.length()+" Byte(s)");
        List<Integer> pdfLengthList = pdf.readConcatenatedFile(fin);
        if(pdfLengthList.get(0)!=0){ log.write("ERRO","Beginning Mark "+pdf.BOM+" not found"); }
        else{ 
            String fname = getFileName(fin.getName());
            File fout = (bSplitZip && nZipSerial>0) ? new File(outFolder,getSerialFilename(zipName,nZipSerial++)) : new File(outFolder,zipName);
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(fin))) {
                for(int size: pdfLengthList){
                    if(size>0){
                        String pdfName = getPdfFilename(fname,++nInPdf);
                        byte[] buf = new byte[size];
                        if(in.read(buf)!=size){ log.write("ERRO","Insufficient Data in reading"+pdfName); }
                        else if(pdf.readDocumentProperty(buf)==false){ log.write("ERRO","Failed to read Pdf Properties"); }
                        else{
                            String pdfCreationDate = pdf.getCreationDate();
                            int nPdfPageCount = pdf.getPageCount();
                            String pdfKeyword = pdf.getKeywords();
                            if(!readImprint(pdfKeyword,csvLines,pdfName,fname,nPdfPageCount,pdfCreationDate)){ log.write("WARN","Failed to read imprint"); }
                            else{
                                if(bZip){
                                    if(bSplitZip && nZipSerial>0 && zip.isOversize(nMaxZipEntry,lMaxZipSize)){ 
                                        fout = new File(outFolder,getSerialFilename(zipName,nZipSerial++));
                                    }
                                    if(zip.isNewFile(fout)){ outputFileList.add(fout); }
                                    if(this.bPdfEncrypt){ 
                                        byte[] d;
                                        if((d=pdfEncrypt(buf))!=null){ log.write("Encrypt "+pdfName); buf=d; }
                                        else{ log.write("WARN","Failed to encrypt "+pdfName); }
                                    }
                                    zip.write(fout,pdfName,buf);
                                }
                                else{ 
                                    fout = new File(outFolder,pdfName); 
                                    outputFile(fout,buf); 
                                    outputFileList.add(fout);
                                }
                                nOutPdf++;
                            }                                
                        }
                    }
                }
                in.close();
            } catch (IOException ex) { System.out.println(ex.getMessage()); }
            log.audit(fname+",PDF,"+nOutPdf+",Z");
        }
        if(nInPdf>0){ log.write("Read "+getPathName(fin)+ " Total "+nInPdf+" Pdf(s)"); }
        if(nOutPdf>0){ log.write("Write "+getPathName(fin) + " Total "+nOutPdf+" Pdf(s)"); }
        return(outputFileList);
    }
    byte[] pdfEncrypt(byte[] s){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            PdfReader reader = new PdfReader(s);
            PdfStamper stamper = new PdfStamper(reader, out);
            stamper.setEncryption(null, "".getBytes(),
                PdfWriter.ALLOW_PRINTING | PdfWriter.ALLOW_SCREENREADERS
                , PdfWriter.STANDARD_ENCRYPTION_128
            );
            stamper.close();
            reader.close();
            return(out.toByteArray());
        } 
        catch (IOException | DocumentException ex) { System.out.println(ex.getMessage()); return(null); }
    }
    String getPdfFilename(String fname,int nPdf){ return(fname+"-"+String.format("%05d.pdf",nPdf)); }
    String getSerialFilename(String fname,int nSerial){
        String name=fname,ext = "";
        int n;
        if((n=fname.lastIndexOf('.'))>0){ ext="."+fname.substring(n+1); name=fname.substring(0,n); }
        return(String.format(name+".%02d"+ext,nSerial));
    }
    
    boolean readImprint(String s,List<String> csvLines,String pdfName,String fname,int nPdfPageCount,String pdfCreationDate){
        String DOC_TYPE = s.substring(6,7);
        boolean rc;
        switch(DOC_TYPE){
            case "1" : rc=readPFImprint(s,csvLines,pdfName,fname,nPdfPageCount,pdfCreationDate); break;
            case "5" : rc=readPTImprint(s,csvLines,pdfName,fname,nPdfPageCount,pdfCreationDate); break;
            case "6" : 
            default  : rc=readCTImprint(s,csvLines,pdfName,fname,nPdfPageCount,pdfCreationDate); break;
        }
        return(rc);
    }
    boolean readCTImprint(String s,List<String> csvLines,String pdfName,String fname,int nPdfPageCount,String pdfCreationDate){ 
        boolean rc;
        int n=0;
        n+=6; 
        String DOC_TYPE = s.substring(n,++n);
        n+=1; 
        String PRN = s.substring(n,n+=9);
        String ASM_YEAR = s.substring(n,n+=4);
        n+=1; 
        String DOC_NATURE = s.substring(n,++n);
        String DOC_GROUP = s.substring(n,++n);
        String TAX_TYPE = s.substring(n,++n);
        String BRN = s.substring(n,n+=9);
        String ASS_NATURE = s.substring(n,++n);
        String NOTICE_TYPE = s.substring(n,++n);
        String ASS_TYPE = s.substring(n,++n);
        String CHARGE_NUM = s.substring(n,n+=7);
        String CHARGE_NUM_CD = s.substring(n,++n);
        String ISSUE_DATE = s.substring(n,n+=8);
        String ASS_SEQ_NUM = s.substring(n,n+=2);
        String ORIGINATED_PRN = s.substring(n,n+=9);
        String ORIGINATED_ASS_SEQ = s.substring(n,n+=2);
        String TOTAL_PAGE_NUM = s.substring(n,n+=2);
        n+=1;
        String ATTACH_IND = s.substring(n,n+=2);
        n+=1;
        String FORM_ID = s.substring(n,n+=8);

        int nTotalPageNum = Integer.parseInt(TOTAL_PAGE_NUM);
        if(nTotalPageNum!=nPdfPageCount){ 
            log.write("ERRO","Ignore "+pdfName+": Inconsistent Total Page Number ("+nTotalPageNum+","+nPdfPageCount+")");
            rc = false;
        }
        else{
            String OUTPUT_FILE = pdfName;
            boolean bBatch = fname.indexOf("BAT")==3 ? true : false;
            String LAST_PAGE = Integer.toString(nPdfPageCount);
            String ProcessDate = pdfCreationDate; 
            String output_path = "/ocs/capdata/imagepool";
            String allot_batch_no = (bBatch) ? "AFP99" : "PCL99";
            String doc_type = (DOC_TYPE.equals("6")) ? "CTR" : DOC_TYPE;
            String doc_group = "";
            String doc_group_id  = "";
            if(DOC_GROUP.equals("A")){ doc_group_id="111"; }
            else{ doc_group=DOC_GROUP; }
            String doc_nature = "";
            String doc_nature_id = "";
            switch(DOC_NATURE){
                case "A" : doc_nature_id="6150"; break;
                case "R" : doc_nature_id="6151"; break;
                default  : doc_nature=DOC_NATURE; break;
            }
            String asm_year = (ASM_YEAR.equals("0000")) ? "" : ASM_YEAR;
            String issue_date = (ISSUE_DATE.equals("00000000")) ? "" : ISSUE_DATE;
            String tax_type;
            switch(TAX_TYPE){
                case "3" : tax_type="PF"; break;
                case "6" : tax_type="PA"; break;
                case "7" : tax_type="PT"; break;
                case "9" : tax_type="ST"; break;
                default  : tax_type=""; break;
            }
            String asm_type;
            switch(ASS_NATURE){
                case "O" : asm_type="Original"; break;
                case "A" : asm_type="Addt'l 1"; break;
                case "R" : asm_type="Revised"; break;
                case "E" : asm_type="EA"; break;
                case "P" : asm_type="PAD/PAW"; break;
                case "N" : asm_type="Not Asst"; break;
                case "0" : asm_type=""; break;
                default : asm_type = ASS_NATURE; break;
            }
            String brn = (BRN.equals("000000000")) ? "" : BRN;
            String user_id = (bBatch) ? "BATCH" : "ONLINE";
            String time_stamp = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
            String notice_type_id = (NOTICE_TYPE.equals("0")) ? "" : NOTICE_TYPE;
            String fp_type_id = (!ASS_TYPE.equals("1") && !ASS_TYPE.equals("2") && !ASS_TYPE.equals("3")) ? "" : ASS_TYPE;
            String charge_no = "";
            if(!CHARGE_NUM.equals("0000000")){
               String yy = "##";
               if(!FORM_ID.equals("IRC6425") && !FORM_ID.equals("IRC6426") && !FORM_ID.equals("IRC6443")){
                   yy = ASM_YEAR.substring(2,4);
               }
               charge_no = TAX_TYPE + "-" + CHARGE_NUM + "-" + yy + "-" + CHARGE_NUM_CD;
            }
            String ass_seq_no = (ASS_SEQ_NUM.equals("00")) ? "" : ASS_SEQ_NUM;
            String originated_prn = (ORIGINATED_PRN.equals("000000000")) ? "" : ORIGINATED_PRN;
            String originated_ass_seq_no = (ORIGINATED_ASS_SEQ.equals("00")) ? "" : ORIGINATED_ASS_SEQ.trim();
            String csvCTLine = output_path + "," + OUTPUT_FILE + "," + 
                    allot_batch_no + ",1,"+LAST_PAGE+",,,,,N," +
                    ProcessDate + ",,M,P," + doc_type + "," + 
                    doc_group + "," + doc_group_id + "," + doc_nature + "," + doc_nature_id + ",," + 
                    asm_year + ",,," + issue_date + ",N," + PRN + ",,," + 
                    tax_type + "," + asm_type + "," + brn + ",,,,," +
                    time_stamp + "," +
                    user_id + ",," + FORM_ID + "," + notice_type_id + "," + fp_type_id + "," +
                    charge_no + "," + ass_seq_no + ",,," + originated_prn + "," +
                    originated_ass_seq_no;
            csvLines.add("\""+csvCTLine.replace(",","\",\"")+"\"");
            rc = true;
        }
        return(rc);
    }
    boolean readPTImprint(String s,List<String> csvLines,String pdfName,String fname,int nPdfPageCount,String pdfCreationDate){ 
        boolean rc;
        int n=0;
        n+=6; 
        String DOC_TYPE = s.substring(n,++n);
        n+=1; 
        String PUN = s.substring(n,n+=14);
        String OC = s.substring(n,n+=4);
        String ASM_YEAR = s.substring(n,n+=4);
        n+=1; 
        String DOC_NATURE = s.substring(n,++n);
        String DOC_GROUP = s.substring(n,++n);
        String NOTICE_TYPE = s.substring(n,++n);
        String ASS_TYPE = s.substring(n,++n);
        String CHARGE_NUM = s.substring(n,n+=7);
        String CHARGE_NUM_CD = s.substring(n,++n);
        String ISSUE_DATE = s.substring(n,n+=8);
        String ASS_SEQ_NUM = s.substring(n,n+=2);
        String TOTAL_PAGE_NUM = s.substring(n,n+=2);
        n+=1;
        String ATTACH_IND = s.substring(n,n+=2);
        n+=1;
        String FORM_ID = s.substring(n,n+=8);

        int nTotalPageNum = Integer.parseInt(TOTAL_PAGE_NUM);
        if(nTotalPageNum!=nPdfPageCount){ 
            log.write("ERRO","Ignore "+pdfName+": Inconsistent Total Page Number ("+nTotalPageNum+","+nPdfPageCount+")");
            rc = false;
        }
        else{
            String OUTPUT_FILE = pdfName;
            boolean bBatch = fname.indexOf("BAT")==3 ? true : false;
            String LAST_PAGE = Integer.toString(nPdfPageCount);
            String ProcessDate = pdfCreationDate; 
            String output_path = "/ocs/capdata/imagepool";
            String allot_batch_no = (bBatch) ? "AFP99" : "PCL99";
            String doc_type = (DOC_TYPE.equals("5")) ? "PT" : DOC_TYPE;
            String doc_group = "";
            String doc_group_id  = "";
            if(DOC_GROUP.equals("A")){ doc_group_id="309"; }
            else{ doc_group=DOC_GROUP; }
            String doc_nature = "";
            String doc_nature_id = "";
            switch(DOC_NATURE){
                case "A" : doc_nature_id="5136"; break;
                case "R" : doc_nature_id="5137"; break;
                default  : doc_nature=DOC_NATURE; break;
            }
            String asm_year = (ASM_YEAR.equals("0000")) ? "" : ASM_YEAR;
            String issue_date = (ISSUE_DATE.equals("00000000")) ? "" : ISSUE_DATE;
            String asm_type = "";
            String user_id = (bBatch) ? "BATCH" : "ONLINE";
            String time_stamp = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
            String notice_type_id = NOTICE_TYPE;
            String fp_type_id = (!ASS_TYPE.equals("1") && !ASS_TYPE.equals("2") && !ASS_TYPE.equals("3")) ? "" : ASS_TYPE;
            String charge_no = "";
            if(!CHARGE_NUM.equals("0000000")){
               String yy = "##";
               if(ASM_YEAR.length()>=4){
                   yy = ASM_YEAR.substring(2,4);
               }
               charge_no = "5-" + CHARGE_NUM + "-" + yy + "-" + CHARGE_NUM_CD;
            }
            String ass_seq_no = (ASS_SEQ_NUM.equals("00")) ? "" : ASS_SEQ_NUM;
            String csvPTLine = output_path + "," + OUTPUT_FILE + "," + 
                    allot_batch_no + ",1,"+LAST_PAGE+",,,,,N," +
                    ProcessDate + ",,M,P," + doc_type + "," + 
                    doc_group + "," + doc_group_id + "," + doc_nature + "," + doc_nature_id + ",," + 
                    asm_year + ","+ asm_type + ",,," + issue_date + ",N," + PUN + "," + OC +  ",,,,,," +
                    time_stamp + "," +
                    user_id + ",," + FORM_ID + "," + charge_no + "," + notice_type_id + "," + fp_type_id + "," +
                    ass_seq_no;
            csvLines.add("\""+csvPTLine.replace(",","\",\"")+"\"");
            rc = true;
        }
        return(rc);
    }
    boolean readPFImprint(String s,List<String> csvLines,String pdfName,String fname,int nPdfPageCount,String pdfCreationDate){
        boolean rc;
        int n=0;
        n+=6;
        String DOC_TYPE = s.substring(n,++n);
        n+=1;
        String BRN = s.substring(n,n+=9);
        String ASM_YEAR = s.substring(n,n+=4);
        n+=1;
        String DOC_NATURE = s.substring(n,++n);
        String DOC_GROUP = s.substring(n,++n);
        String ASS_ACTION = s.substring(n,++n);
        String TRADE_CAT = s.substring(n,n+=2);
        String SECTION = s.substring(n,n+=2);
        String SUB_SECTION = s.substring(n,++n);
        String NOTICE_TYPE = s.substring(n,++n);
        String ASS_TYPE = s.substring(n,++n);
        String CHARGE_NUM = s.substring(n,n+=17);
        String ISSUE_DATE = s.substring(n,n+=8);
        String ASS_SEQ_NUM = s.substring(n,n+=2);
        String TOTAL_PAGE_NUM = s.substring(n,n+=2);
        n+=1;
        String ATTACH_IND = s.substring(n,n+=2);
        n+=1;
        String FORM_ID = s.substring(n,n+=8);

        int nTotalPageNum = Integer.parseInt(TOTAL_PAGE_NUM);
        if(nTotalPageNum!=nPdfPageCount){
            log.write("ERRO","Ignore "+pdfName+": Inconsistent Total Page Number ("+nTotalPageNum+","+nPdfPageCount+")");
            rc = false;
        }
        else{
            String OUTPUT_FILE = pdfName;
            boolean bBatch = fname.indexOf("BAT")==3 ? true : false;
            String LAST_PAGE = Integer.toString(nPdfPageCount);
            String ProcessDate = pdfCreationDate;
            String output_path = "/ocs/capdata/imagepool";
            String allot_batch_no = (bBatch) ? "AFP99" : "PCL99";
            String doc_type = (DOC_TYPE.equals("1")) ? "PF" : DOC_TYPE;
            String doc_group = "";
            String doc_group_id  = "";
            if(DOC_GROUP.equals("D")){ doc_group_id="406"; }
            else{ doc_group=DOC_GROUP; }
            String doc_nature = "";
            String doc_nature_id = "";
            switch(DOC_NATURE){
                case "A" : doc_nature_id="1931"; break;
                case "O" : doc_nature_id="1014"; break;
                case "N" : doc_nature_id="1015"; break;
                default  : doc_nature=DOC_NATURE; break;
            }
            String asm_year = (ASM_YEAR.equals("0000")) ? "" : ASM_YEAR;
            String time_stamp = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
            String issue_date = (ISSUE_DATE.equals("00000000")) ? "" : ISSUE_DATE;
            String ass_action;
            switch(ASS_ACTION){
                case "O" : ass_action="Original"; break;
                case "A" : ass_action="Additional"; break;
                case "R" : ass_action="Revised"; break;
                case "0" : ass_action="N/A or Other"; break;
                default : ass_action = ASS_ACTION; break;
            }
            String charge_num = (CHARGE_NUM.equals("00000000000000000")) ? "" : CHARGE_NUM;
            String notice_type_id = (NOTICE_TYPE.equals("0")) ? "" : NOTICE_TYPE;
            String user_id = (bBatch) ? "BATCH" : "ONLINE";
            String fp_type_id = (!ASS_TYPE.equals("1") && !ASS_TYPE.equals("2") && !ASS_TYPE.equals("3")) ? "" : ASS_TYPE;
            String ass_seq_no = (ASS_SEQ_NUM.equals("00")) ? "" : ASS_SEQ_NUM;

            String csvPFLine = output_path + "," + OUTPUT_FILE + "," +
                    allot_batch_no + ",1,"+LAST_PAGE+",,,,,N," +
                    ProcessDate + ",,M,I," + doc_type + "," +
                    doc_group + "," + doc_group_id + "," +
                    doc_nature + "," + doc_nature_id + ",," +
                    asm_year + "," + 
                    BRN + "," + 
                    time_stamp + "," + 
                    user_id + ",," + FORM_ID + "," + issue_date + "," + 
                    ass_action + "," + TRADE_CAT + "," + SECTION + "," + SUB_SECTION + "," +
                    charge_num + "," + notice_type_id + "," + fp_type_id + "," +
                    ass_seq_no;
            csvLines.add("\""+csvPFLine.replace(",","\",\"")+"\"");
            rc = true;
        }
        return(rc);
    }
    final String[] ctCsvFieldTitle = {
        "OUTPUT_PATH","OUTPUT_FILE","ALLOT_BATCH_NO","FIRST_PAGE","LAST_PAGE",
        "SCN_TIME","COR_TIME","MUI_TIME","SMP_TIME","REJECT",
        "CAPTURE_DATE","TO_EXTAPP","FROM_EXTAPP","CLASSIFICATION","DOC_TYPE",
        "DOC_GROUP","DOC_GROUP_ID","DOC_NATURE","DOC_NATURE_ID","SECTION_CODE",
        "ASM_YEAR","RECEIPT_DATE","NOTE_RECEIPT","ISSUE_DATE","SUPPLMENT",
        "PRN","RETURN_TYPE","TT_PIN_ISSUE_MODE","TAX_TYPE","ASM_TYPE",
        "BRN","TWO_DIGIT_YEAR_CODE","SERIAL_NO","MEMO_SERIAL_NO","ATTACH_SERIAL_NO",
        "TIME_STAMP","USER_ID","NOTE","FORM_ID","NOTICE_TYPE_ID",
        "FP_TYPE_ID","CHARGE_NO","ASS_SEQ_NO","CLAIM_NATURE_ID","TRN",
        "ORIGINATED_PRN","ORIGINATED_ASS_SEQ_NO"
    };
    final String[] ptCsvFieldTitle = {
        "OUTPUT_PATH","OUTPUT_FILE","ALLOT_BATCH_NO","FIRST_PAGE","LAST_PAGE",
        "SCN_TIME","COR_TIME","MUI_TIME","SMP_TIME","REJECT",
        "CAPTURE_DATE","TO_EXTAPP","FROM_EXTAPP","CLASSIFICATION","DOC_TYPE",
        "DOC_GROUP","DOC_GROUP_ID","DOC_NATURE","DOC_NATURE_ID","SECTION_CODE",
        "ASM_YEAR","ASM_TYPE",
        "RECEIPT_DATE","NOTE_RECEIPT","ISSUE_DATE","SUPPLMENT",
        "PUN","OC","ISSUE_MONTH",
        "TWO_DIGIT_YEAR_CODE","SERIAL_NO","MEMO_SERIAL_NO","ATTACH_SERIAL_NO",
        "TIME_STAMP","USER_ID","NOTE","FORM_ID",
        "CHARGE_NO",
        "NOTICE_TYPE_ID",
        "FP_TYPE_ID",
        "ASS_SEQ_NO",
    };
    final String[] pfCsvFieldTitle = {
        "OUTPUT_PATH","OUTPUT_FILE","ALLOT_BATCH_NO","FIRST_PAGE","LAST_PAGE",
        "SCN_TIME","COR_TIME","MUI_TIME","SMP_TIME","REJECT",
        "CAPTURE_DATE","TO_EXTAPP","FROM_EXTAPP","CLASSIFICATION","DOC_TYPE",
        "DOC_GROUP","DOC_GROUP_ID",
        "DOC_NATURE","DOC_NATURE_ID","SECTION_CODE",
        "ASM_YEAR",
        "BRN","TIME_STAMP","USER_ID","NOTE","FORM_ID","ISSUE_DATE",
        "ASS_ACTION","TRADE_CAT","SECTION","SUB_SECTION",
        "CHARGE_NO",
        "NOTICE_TYPE_ID",
        "FP_TYPE_ID",
        "ASS_SEQ_NO"
    };
    String getDocType(String csvLine){ String[] s = csvLine.split(","); return((s.length>14) ? s[14].replace("\"","") : ""); }
    byte[] formatCsv(String[] csvFieldTitle,ByteArrayOutputStream bout,List<String> csvLines){
        if(csvLines.size()>0){ 
            try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(bout))) {
                if(csvFieldTitle!=null){ 
                    String csvHeader = null;
                    for(String s:csvFieldTitle){ 
                        if(csvHeader==null){ csvHeader = "\""+ s + "\""; }
                        else{ csvHeader += ",\""+ s + "\""; }
                    }
                    out.write(csvHeader); out.newLine(); 
                }
                for(String s : csvLines){ out.write(s); out.newLine(); }
                out.flush();
                out.close();
            }
            catch(IOException ex){ System.out.println(ex.getMessage()); }
        }
        return(bout.toByteArray());
    }
    List<File> readExtFileList(File fin,File inFolder,String fileExt){
        List<File> extFileList = new ArrayList<>();
        boolean rc=false;
        int nRetry=3;
        while(rc==false && nRetry>0){
            try (BufferedReader in = new BufferedReader(new FileReader(fin))) {
                String s;
                while((s=in.readLine())!=null){
                    String d = s.trim();
                    if(s.isEmpty() || s.startsWith("//") || s.startsWith("#")){}
                    else if(s.toUpperCase().endsWith(fileExt.toUpperCase())){
                        File file = new File(inFolder,s);
                        if(!file.exists()){ log.write("WARN",file.getCanonicalPath()+" does not exist"); }
                        else if(!file.isFile()){ log.write("WARN",file.getCanonicalPath()+" is not a file"); }
                        else if(file.length()==0){ log.write("WARN",file.getCanonicalPath()+" is empty"); }
                        else{ extFileList.add(file); }
                    }
                    else if(bVerbose){ System.out.println(s+" does not ended with "+fileExt); }
                }
                in.close();
                rc=true;
            }
            catch (IOException ex) { System.out.println(ex.getMessage()); rc=false; } 
            nRetry--;            
        }
        if(rc==false){ log.write("WARN","Failed to process "+getPathName(fin)); }
        return(extFileList);
    }

    protected String tmpPath = ".";
    protected String backupPath = "";
    protected String logFile = "";
    protected String auditFile = "";
    protected boolean bLogDisplay = true;
    protected boolean bVerbose;
    protected Ini ini = new Ini();
    protected Log log = new Log();
    protected Bku bku = new Bku();
    protected Pdf pdf = new Pdf();
    protected Zip zip = new Zip();
    protected boolean bBackup = false;
    
    protected void initEps(String backupPath,String logFile,String auditFile,boolean bLogDisplay,boolean bVerbose){
        // for V1.2 and below
        this.backupPath = backupPath;
        this.logFile = logFile;
        this.auditFile = auditFile;
        this.bLogDisplay = bLogDisplay;
        this.bVerbose = bVerbose;
        initEps();
    }
    protected void initEps(String tmpPath,String backupPath,String logFile,String auditFile,boolean bLogDisplay,boolean bVerbose){
        // for V1.3 and above
        this.tmpPath = tmpPath.isEmpty() ? "." : tmpPath;
        initEps(backupPath,logFile,auditFile,bLogDisplay,bVerbose);
    }
    private void initEps(){
        // initEps
        log.init(this.toString(),this.logFile, this.auditFile, this.bLogDisplay);
        bku.setLog(log); bku.setBackupPath(backupPath); 
        bBackup = !backupPath.isEmpty();
        zip.setLog(log);
    }    
    //
    // common functions
    //
    static protected String getPathName(File file){
        try{ return(file.getCanonicalPath()); } 
        catch (IOException ex) { System.out.println(ex.getMessage()); return(file.getAbsolutePath()); }
    }
    static protected boolean createEmptyFile(File file){
        try{ return(file.createNewFile()); } 
        catch (IOException ex) { System.out.println(ex.getMessage()); return(false); }
    }
    static protected long day2msec(int nDay){ return(((long)nDay)*24*60*60*1000); }
    static protected long min2msec(int nMin){ return(((long)nMin)*60*1000); }
    static protected boolean isExpiredFile(File f,long msec){ 
        if(msec<0 || !f.canRead() || !f.canWrite() ){ return(false); }
        else{ return(Calendar.getInstance().getTime().after(new Date(f.lastModified()+msec))); } 
    }
    protected boolean isExpiredFile(File[] files,long msec){ 
        for(File f:files){ 
            if(!isExpiredFile(f,msec)){ return(false); } 
        } 
        return(true); 
    }
    protected boolean isExpiredGroupFile(File[] files,File file,long msec){ 
        String fname = getFileName(file.getName()).toUpperCase();
        for(File f:files){ 
            if(f.getName().toUpperCase().startsWith(fname)){
                if(!isExpiredFile(f,msec)){ 
                    System.out.println(f.getName()+" is still active");
                    return(false); 
                }
            }
        } 
        return(true); 
    }
    protected boolean isExpiredGroupFile(File[] files,boolean[] bExpired,File file){ 
        String fname = getFileName(file.getName()).toUpperCase();
        int n=0;
        for(File f:files){ 
            if(f.getName().toUpperCase().startsWith(fname)){
                // if(bExpired[n]){ 
                if(!bExpired[n]){ 
                    System.out.println(f.getName()+" is still active");
                    return(false); 
                }
            }
            n++;
        } 
        return(true); 
    }
       
    protected boolean runCommand(String command){
        boolean rc = true;
        if(bVerbose){ System.out.println(command); }
        String[] cmdArgs = command.split(" ");
        return(cmdArgs.length>0 ? runCommand(cmdArgs) : false);
    }
    protected boolean runCommand(String[] cmdArgs) {
        boolean rc = true;
        File ftmp = null;
        ProcessBuilder pb = new ProcessBuilder(cmdArgs);
        try{
            String s = "";
            for(String cs:pb.command()){ s=s + (s.isEmpty()?"":" ")+cs; }
            System.out.println(s);
            pb.redirectErrorStream(true);
            // pb.redirectOutput(ProcessBuilder.Redirect.to(ftmp = File.createTempFile(this.toString(),".TMP",new File("."))));
            pb.redirectOutput(ProcessBuilder.Redirect.to(ftmp = File.createTempFile(this.toString(),".TMP",new File(tmpPath))));
            Process process = pb.start();
            System.out.println("Waiting RunCommand Process to end");
            process.waitFor();
            System.out.println("RunCommand Process ended");
            try (BufferedReader in = new BufferedReader(new FileReader(ftmp))) {
                while((s=in.readLine())!=null){ if(!s.isEmpty()){ log.write(cmdArgs[0]+" : "+s); } }
            }
            System.out.println("Destroy RunCommand Process");
            process.destroy();
        }
        catch (IOException | InterruptedException e) { System.out.println(e.getMessage()); rc=false; } 
        if(ftmp!=null && ftmp.exists()){ ftmp.delete(); }
        return(rc);
    }

    protected List<String> readLstFile(File fin){
        List<String> list = new ArrayList<>();
        boolean rc=false;
        int nRetry=3;
        while(rc==false && nRetry>0){
            try (BufferedReader in = new BufferedReader(new FileReader(fin))) {
                String s;
                while((s=in.readLine())!=null){
                    String d = s.trim();
                    if(!d.isEmpty()){ list.add(d); }
                }
                in.close();
                rc=true;
            }
            catch (IOException ex) { System.out.println(ex.getMessage()); rc=false; } 
            nRetry--;            
        }
        if(rc==false){ log.write("WARN","Failed to read "+getPathName(fin)); }
        return(list);        
    }
    protected void outputLstFile(File fout,List<String> list){
        boolean rc=false;
        int nRetry=3;
        if(list.size()>0){ 
            while(rc==false && nRetry>0){
                try (BufferedWriter out = new BufferedWriter(new FileWriter(fout))) {
                    for(String s : list){ out.write(s); out.newLine(); }
                    out.flush();
                    out.close();
                    rc=true;
                }
                catch (IOException ex) { System.out.println(ex.getMessage()); rc=false; } 
                nRetry--;
            }
        }
        if(rc){ log.write("Write "+getPathName(fout)+" " + list.size() + " Line(s)"); }
        else{ log.write("WARN","Failed to write "+getPathName(fout)); }
    }
    protected void outputFile(File fout,byte[] buf){
        boolean rc=false; 
        int nRetry = 3;
        while(rc==false && nRetry>0){
            try (FileOutputStream out = new FileOutputStream(fout)) {
                out.write(buf);
                out.flush();
                out.close();
                rc=true;
            } 
            catch (IOException ex) { System.out.println(ex.getMessage()); rc=false; } 
            nRetry--;
        }
        if(rc){ log.write("Write "+fout.getName()+" "+fout.length()+" Byte(s)"); }
        else{ log.write("WARN","Failed to write "+getPathName(fout)); }
    }
    
    protected String getMarkFileName(String s,String mark){
        String d;
        int n = s.lastIndexOf('.');
        if(n==0){ d = mark + s; }
        else if(n==s.length()-1){ d = s + mark; }
        else if(n>0){ d = s.substring(0,n) + "."+ mark + "." + s.substring(n+1); }
        else { d = s + "." + mark; }
        return d;
    }
    protected String getFileName(String s){
        int n = s.lastIndexOf('.');
        return ((n==s.length()-1 || n>=0) ? s.substring(0,n) :s);
    }
    protected String getFileExt(String s){
        int n = s.lastIndexOf('.');
        return (n>0 ? s.substring(n+1) : "UNKWON");
    }
    private String replaceDynamicName(String name,Date date){
        String d = new SimpleDateFormat("yyyyMMdd").format(date);
        String s = name.replace("{YYYYMMDD}",d);
        s = s.replace("{YYYYMM}",d.substring(0, 6));
        s = s.replace("{YYYY}",d.substring(0, 4));
        s = s.replace("{MM}",d.substring(4, 6));
        s = s.replace("{DD}",d.substring(6, 8));        
        return(s);
    }
    protected File getDynamicFile(String fname){
        if(fname==null){ return(null); }
        else{
            // String s = fname.replace("{YYYYMMDD}",new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()));
            String s = replaceDynamicName(fname,Calendar.getInstance().getTime());
            File file = new File(s.replace("\"","").replace("\\\\",File.separator));
            return(file);
        }
    }
    protected File getDynamicFile(String fname,File fsrc,String ext){
        String foldername = fsrc.getParent();
        String fullname = fsrc.getName();
        String filename = fullname.toUpperCase().endsWith("."+ext.toUpperCase()) ? fullname.substring(0,fullname.length()-ext.length()-1): getFileName(fullname);
        if(fname==null){ return(null); }
        else{
            //String s = fname.replace("{YYYYMMDD}",new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()));
            String s = replaceDynamicName(fname,Calendar.getInstance().getTime());
            s = s.toUpperCase().replace("{FILE}", filename).replace("{FILE.EXT}", fullname);
            File file = new File(s.replace("\"","").replace("\\\\",File.separator));
            return(file);
        }
    }
    protected File getDynamicFile(String fname,File fsrc){
        String foldername = fsrc.getParent();
        String fullname = fsrc.getName();
        String filename = getFileName(fullname);
        if(fname==null){ return(null); }
        else{
            // String s = fname.replace("{YYYYMMDD}",new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()));
            String s = replaceDynamicName(fname,Calendar.getInstance().getTime());
            s = s.toUpperCase().replace("{FILE}", filename).replace("{FILE.EXT}", fullname);
            File file = new File(s.replace("\"","").replace("\\\\",File.separator));
            return(file);
        }
    }
    protected File getDynamicInFolder(String inputPath){
        File inFolder = getDynamicFile(inputPath);
        if(inFolder==null){ log.write("Input Folder not defined"); }
        else if(!inFolder.exists()){ log.write("WARN",getPathName(inFolder)+" does not exist"); inFolder=null; }
        else if(!inFolder.isDirectory()){ log.write("WARN",getPathName(inFolder)+" is not a folder"); inFolder=null; }
        return(inFolder);
    }
    protected File getDynamicOutFolder(String outputPath){
        File outFolder = getDynamicFile(outputPath);
        if(!outFolder.exists()){ 
            if(outFolder.mkdirs()){ log.write("Create Folder "+getPathName(outFolder)); }
            else{ log.write("WARN","Failed to create Folder "+getPathName(outFolder)); }
        }                           
        if(!outFolder.exists()){ log.write("WARN",getPathName(outFolder)+" does not exist"); outFolder=null; }
        else if(!outFolder.isDirectory()){ log.write("WARN",getPathName(outFolder)+" is not a folder"); outFolder=null;}
        return(outFolder);
    }
    protected File getDynamicOutFolder(String outputPath,File fsrc){
        File outFolder = getDynamicFile(outputPath,fsrc);
        if(!outFolder.exists()){ 
            if(outFolder.mkdirs()){ log.write("Create Folder "+getPathName(outFolder)); }
            else{ log.write("WARN","Failed to create Folder "+getPathName(outFolder)); }
        }                           
        if(!outFolder.exists()){ log.write("WARN",getPathName(outFolder)+" does not exist"); outFolder=null; }
        else if(!outFolder.isDirectory()){ log.write("WARN",getPathName(outFolder)+" is not a folder"); outFolder=null;}
        return(outFolder);
    }

}
