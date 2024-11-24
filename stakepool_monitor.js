const { Connection, PublicKey } = require('@solana/web3.js');
const solanaStakePool = require ('@solana/spl-stake-pool');
const { Client } = require('pg');

const pgClient = new Client({
    user: 'postgres',
    host: process.env.PGSQL_HOST,
    database: process.env.DB,
    password: process.env.PGSQL_PASSWD,
    port: 5432,
});

// Connect to PostgreSQL
pgClient.connect()
    .then(() => console.log('Connected to PostgreSQL'))
    .catch(err => console.error('PostgreSQL connection error', err));

// Solana configuration
const connection = new Connection(process.env.SOLANA_ENDPOINT, 'confirmed')

// Function to fetch and write data
async function fetchDataAndStore() {
    try {

        const stake_pool_account = await solanaStakePool.getStakePoolAccount(connection, new PublicKey(process.env.STAKEPOOL));

        const query = `
        INSERT INTO bbsol_stakepool (
            pubkey, lamports, own3r, accounttype, manager, staker, stakedepositauthority, stakewithdrawbumpseed,
            validatorlist, reservestake, poolmint, managerfeeaccount, tokenprogramid, totallamports, pooltokensupply,
            lastupdateepoch, lockup, epochfee, nextepochfee, preferreddepositvalidatorvoteaddress, preferredwithdrawvalidatorvoteaddress,
            stakedepositfee, stakewithdrawalfee, nextstakewithdrawalfee, stakereferralfee, soldepositauthority, soldepositfee, solreferralfee,
            solwithdrawauthority, solwithdrawalfee, nextsolwithdrawalfee, lastepochpooltokensupply, lastepochtotallamports, collected_time
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34);
        `;

        const values = [
            stake_pool_account.pubkey.toString(),
            stake_pool_account.account.lamports.toString(),
            stake_pool_account.account.owner.toString(),
            Number(stake_pool_account.account.data.accountType),
            stake_pool_account.account.data.manager.toString(),
            stake_pool_account.account.data.staker.toString(),
            stake_pool_account.account.data.stakeDepositAuthority.toString(),
            stake_pool_account.account.data.stakeWithdrawBumpSeed,
            stake_pool_account.account.data.validatorList.toString(),
            stake_pool_account.account.data.reserveStake.toString(),
            stake_pool_account.account.data.poolMint.toString(),
            stake_pool_account.account.data.managerFeeAccount.toString(),
            stake_pool_account.account.data.tokenProgramId.toString(),
            stake_pool_account.account.data.totalLamports.toString(),
            stake_pool_account.account.data.poolTokenSupply.toString(),
            stake_pool_account.account.data.lastUpdateEpoch.toString(),
            stake_pool_account.account.data.lockup,
            stake_pool_account.account.data.epochFee,
            stake_pool_account.account.data.nextEpochFee,
            stake_pool_account.account.data.preferredDepositValidatorVoteAddress ? stake_pool_account.account.data.preferredDepositValidatorVoteAddress.toString() : null,
            stake_pool_account.account.data.preferredWithdrawValidatorVoteAddress ? stake_pool_account.account.data.preferredWithdrawValidatorVoteAddress.toString() : null,
            stake_pool_account.account.data.stakeDepositFee,
            stake_pool_account.account.data.stakeWithdrawalFee,
            stake_pool_account.account.data.nextStakeWithdrawalFee,
            stake_pool_account.account.data.stakeReferralFee.toString(),
            stake_pool_account.account.data.solDepositAuthority ? stake_pool_account.account.data.solDepositAuthority.toString() : null,
            stake_pool_account.account.data.solDepositFee,
            stake_pool_account.account.data.solReferralFee,
            stake_pool_account.account.data.solWithdrawAuthority ? stake_pool_account.data.solWithdrawAuthority.toString() : null,
            stake_pool_account.account.data.solWithdrawalFee,
            stake_pool_account.account.data.nextSolWithdrawalFee,
            stake_pool_account.account.data.lastEpochPoolTokenSupply.toString(),
            stake_pool_account.account.data.lastEpochTotalLamports.toString(),
            new Date().toISOString()
        ]
    

        // Insert data into PostgreSQL
        await pgClient.query(query, values);
        console.log('Data written to PostgreSQL');
    } catch (error) {
        console.error('Error fetching or storing data:', error);
    }
}

// Function to align fetching with the clock
function scheduleDataFetch() {
    const now = new Date();
    const seconds = now.getSeconds();

    let delay;
    if (seconds < 30) {
        // Calculate delay until the next first-second of the minute
        delay = (30 - seconds) * 1000;
    } else {
        // Calculate delay until the next first-second of the next minute
        delay = (60 - seconds) * 1000;
    }

    setTimeout(async () => {
        // Fetch data at the scheduled time
        await fetchDataAndStore();

        // Immediately schedule the next fetch 30 seconds later
        setTimeout(async () => {
            await fetchDataAndStore();
            // Reschedule the entire process
            scheduleDataFetch();
        }, 30 * 1000);
    }, delay);
}

// Start the process
scheduleDataFetch();

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('Shutting down...');
    await pgClient.end();
    process.exit();
});
