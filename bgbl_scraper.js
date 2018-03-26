const https = require('https')
const url = require('url')
const fs = require('fs')
const path = require('path')

// own implementation to avoid a huge dependency tree (like the `request` module)
const request = (options) => new Promise((resolve, reject) => {
    https.get(options, (res) => {
        if(res.statusCode != 200) reject(new Error('Failed to load page, status code: ' + res.statusCode))
        const body = []
        res.on('data', (chunk) => body.push(chunk))
        res.on('end', () => resolve({
            headers: res.headers,
            body: body.join('')
        }))
    })
    .on('error', (err) => reject(err))
})
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const romanNumerialsMap = {'I':1, 'II':2, 'III':3, 'IV':4, 'V':5}

// ====================================

class BGBLScraper {
    constructor() {
        this.cookies = null
        this.isLoggingIn = false
        this.BASE_URL = 'https://www.bgbl.de/xaver/bgbl'
    }
 
    async run(outfile, minyear, maxyear) {
        let collection = {}

        // load outfile if it exists
        try {
            collection = require(outfile[0] != '/' ? './'+outfile : outfile)
            console.log('existing file loaded from '+outfile)
        } catch(err) {
            if(err.code != 'MODULE_NOT_FOUND') throw err
        }

        // get ids of both parts/bundesgesetzblätter
        const parts = (await this.getItemChildren(0))
            .filter(b => b.l.match(/^Bundesgesetzblatt Teil/))
            .map(b => {
                // adds index to object based on the roman number
                b.index = romanNumerialsMap[b.l.split('Teil ')[1]]
                return b
            })


        let numbers = []
        for(let part of parts) {

            // get all years for this part
            let resPart = await this.getItemChildren(part.id)

            for(let year of resPart) {
                if(parseInt(year.l) < minyear || parseInt(year.l) > maxyear) continue
                console.log(`[BGBl ${part.index}] get ${year.l}`)

                // get all numbers for this year
                const resYear = await this.getItemChildren(year.id)

                for(let number of resYear) {
                    if(!number.c) continue // ignore other entries like 'Zeitliche Übersicht'                        
                    let [,nr,date] = number.l.match(/^Nr. ([0-9]+) vom (.*)/)
                    numbers.push({
                        id: number.id,
                        number: parseInt(nr),
                        date: date,
                        year: parseInt(year.l),
                        part: part.index
                    })
                }
            }
        }

        let index = 0
        let workers = []


        // run multiple workers in parallel
        for(let i=0;i<10;i++) workers.push( (async() => {

            while(index < numbers.length) {

                // catch one number
                const number = numbers[index++] 

                console.log(`${number.part}\t${number.year}\t${number.number}`)

                // get list of announcements
                const res = await this.getContent(number.id)

                // build a key
                const key = `${number.part}_${number.year}_${number.number}`

                // extend objects with year&part and add to collection
                collection[key] = res.map(a => Object.assign(a, {
                    year: number.year,
                    part: number.part
                }))
            }
            
        })())

        // wait for all workers
        await Promise.all(workers)
        
        // save to disk
        console.log('scraping done, write to outfile')
        fs.writeFileSync(outfile, JSON.stringify(collection))

    }

    async login() {
        this.isLoggingIn = true       
        
        let res = await request(this.BASE_URL+'/start.xav')

        this.cookies = res.headers['set-cookie'].map(c => c.replace(/;.*/, ''))  // removes cookie options
        this.isLoggingIn = false
    }

    async get(path) {
        while(this.isLoggingIn) {
            await sleep(100) // wait while another get() is already logging in
        }

        if(!this.cookies) {
            console.log(`logging in...`)
            await this.login()
        }

        let options = url.parse(this.BASE_URL+'/'+path)
        options.headers = {
            Cookie: this.cookies.join('; ')
        }
        const res = await request(options)
        return JSON.parse(res.body)
    }

    // returns all children elements of item id
    async getItemChildren(id) {
        const res = await this.get(`ajax.xav?q=toclevel&bk=bgbl&n=${id}`)
        return res.items[0].c
    }

    async getContent(id) {
        // need to retrieve it from the html content, because the 
        // ajax.xav response does not contain the page numbers
        const res = await this.get(`text.xav?tocid=${id}`)
        const rows = res.innerhtml.match(/<tr>.*?<\/tr>/g)

        let out = []
        for(let row of rows) {
            let line2 = row.match(/<div class="line2">aus Nr. ([0-9]+) vom ([0-9\.]+), Seite ([0-9]+)<\/div>/)
            let link = row.match(/<a href=".*?#(.*?)".*?>(.*?)<\/a>/)
            let law_date = row.match(/<div>([0-9\.]+)<\/div>/)
            if(!line2 || !link) continue

            out.push({
                // raw: row,
                kind: 'entry',
                name: link[2],
                number: parseInt(line2[1]),
                date: line2[2],
                page: parseInt(line2[3]),
                toc_doc_id: id,
                law_date: law_date ? law_date[1] : null,
                href: `https://www.bgbl.de/xaver/bgbl/start.xav?startbk=Bundesanzeiger_BGBl&jumpTo=${link[1]}`
            })
        }
        return out
    }
}

const scraper = new BGBLScraper

scraper.run(
    process.argv[2],
    process.argv[3] ? parseInt(process.argv[3]) : 0,
    process.argv[4] ? parseInt(process.argv[4]) : 10000
)
.catch(console.error)