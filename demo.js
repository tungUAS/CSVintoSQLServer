'use strict';
var express = require("express");
var app = express();
var http = require("http").createServer(app);
const sql = require('mssql');
const fileUpload = require("express-fileupload");


const config = {
    database: "paololog",
    password: "biservice",
    port: 1433,
    server: "134.97.32.11",
    user: "BIService",
    trustServerCertificate: true,
    synchronize: true,  
  };
 
app.set("view engine", "ejs");
app.use(express.json());
app.use(express.urlencoded({extended:false}));
app.use(fileUpload());

http.listen(4000, function () {
    app.get("/", function (request, result) {
        result.render("index");
    });
});

app.post('/bulkSQL',async (req,res) => {
    truncateQuery();
    var buff = Buffer.from(req.files.file.data);
    const data = buff.toString()
    .split('\n') // split string to lines
    .map(e => e.trim()) // remove white spaces for each line
    .map(e => e.split(',').map(e => e.trim())); 
  
    if(data.length<10000){
        //delay(data,1,data.length,config);
        await bulk_sql_async(data.slice(1,data.length),config);
    }else{
        for(let i=1;i<data.length;i=i+10000){
            let y = i+10000;
            if(y>data.length){
                y = data.length-1;
            }
            //delay(data,i,y,config);
            await bulk_sql_async(data.slice(i,y),config);
        }   
    }
    res.end("Success");
});

 
async function truncateQuery() {
    try {
      let pool = await sql.connect(config);
      let res = await pool.request().query("truncate table [paoloLog].[dbo].[my_users_test_250k]");
      return res.recordsets;
    } catch (error) {
      console.log(" mathus-error :" + error);
    }
  }

 async function bulk_sql_async(data,config) {
    try{
        let pool = await sql.connect(config);
        console.log('connected');
        const table = new sql.Table('my_users_test_250k');
        table.create = true;
        
        table.columns.add('name', sql.VarChar(128), { nullable: false });
        table.columns.add('name1', sql.VarChar(128), { nullable: false });
        table.columns.add('name2', sql.VarChar(128), { nullable: false });
        table.columns.add('name3', sql.VarChar(128), { nullable: false });
        table.columns.add('name4', sql.VarChar(128), { nullable: false });
        table.columns.add('name5', sql.VarChar(128), { nullable: false });
        table.columns.add('name6', sql.VarChar(128), { nullable: false });
        table.columns.add('name7', sql.VarChar(128), { nullable: false });
        table.columns.add('name8', sql.VarChar(128), { nullable: false });
        table.columns.add('name9', sql.VarChar(128), { nullable: false });
        table.columns.add('name10', sql.VarChar(128), { nullable: false });
        table.columns.add('name11', sql.VarChar(128), { nullable: false });
        // add here rows to insert into the table
        for(let i = 0;i<data.length;i++){
            table.rows.add(data[i].toString().split(';')[0],
            data[i].toString().split(';')[1],
            data[i].toString().split(';')[2],
            data[i].toString().split(';')[3],
            data[i].toString().split(';')[4],
            data[i].toString().split(';')[5],
            data[i].toString().split(';')[6],
            data[i].toString().split(';')[7],
            data[i].toString().split(';')[8],
            data[i].toString().split(';')[9],
            data[i].toString().split(';')[10],
            data[i].toString().split(';')[11]); 
        }
        
        const request = new sql.Request();
        return request.bulk(table);
    
    } catch (error) {
        console.log(" mathus-error :" + error);
      }
}  


  
console.log("Running on port 4000");