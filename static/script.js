document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const loadingState = document.getElementById('loading-state');
    const resultsContainer = document.getElementById('results-container');
    const resultsBody = document.getElementById('results-body');
    const resetBtn = document.getElementById('reset-btn');
    const decisionFilter = document.getElementById('decision-filter');
    const sharesFilter = document.getElementById('shares-filter');
    const yuutaiFilter = document.getElementById('yuutai-filter');
    const fetchProgress = document.getElementById('fetch-progress');

    let portfolioData = [];
    let currentSortColumn = null;
    let currentSortAsc = true;

    // Drag and Drop Effects
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover'].forEach(eventName => {
        dropZone.addEventListener(eventName, () => dropZone.classList.add('drag-active'), false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, () => dropZone.classList.remove('drag-active'), false);
    });

    // Handle Drop
    dropZone.addEventListener('drop', handleDrop, false);
    
    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        handleFiles(files);
    }

    // Handle Click Upload
    fileInput.addEventListener('change', function() {
        if(this.files.length) {
            handleFiles(this.files);
        }
    });

    function handleFiles(files) {
        if (files.length === 0) return;
        const file = files[0];
        if (!file.name.endsWith('.csv')) {
            alert('アップロードするファイルはCSV形式である必要があります。');
            return;
        }
        uploadFile(file);
    }

    async function uploadFile(file) {
        dropZone.classList.add('hidden');
        resultsContainer.classList.add('hidden');
        loadingState.classList.remove('hidden');

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errData = await response.json();
                throw new Error(errData.detail || 'Failed to upload CSV. Ensure it is correct.');
            }

            const data = await response.json();
            renderResults(data);
            startSequentialFetches();
        } catch (error) {
            console.error('Error:', error);
            alert(`Error: ${error.message}`);
            // Reset UI on error
            loadingState.classList.add('hidden');
            dropZone.classList.remove('hidden');
        }
    }

    function parseNumeric(val) {
        if (!val || val === "-" || val === "不明" || val === "取得中" || val === "エラー") return -Infinity;
        // 数字、ドット、マイナス、プラス以外をすべて除去
        const num = parseFloat(String(val).replace(/[^0-9.\-+]/g, ''));
        return isNaN(num) ? -Infinity : num;
    }

    /** 取得進捗を更新してタイトル横に表示 */
    function updateFetchProgress() {
        const total = portfolioData.length;
        if (total === 0) { fetchProgress.textContent = ''; return; }
        const fetched = portfolioData.filter(s => s.per !== '取得中').length;
        const pct = Math.round(fetched / total * 100);
        fetchProgress.textContent = `（${fetched}/${total}　${pct}%）`;
    }

    /** 保有株数を数値として返す（不明・"-"は null） */
    function parseShares(val) {
        if (!val || val === '-' || val === '取得中' || val === '不明') return null;
        const n = parseFloat(String(val).replace(/,/g, ''));
        return isNaN(n) ? null : n;
    }

    function renderFilteredAndSortedResults() {
        let filteredData = [...portfolioData];

        // 投資判断フィルタ
        const decisionVal = decisionFilter.value;
        if (decisionVal === '優待？') {
            // 「優待？」は保有株数<100で判定
            filteredData = filteredData.filter(item => {
                const n = parseShares(item.shares);
                return n !== null && n < 100;
            });
        } else if (decisionVal !== 'all') {
            filteredData = filteredData.filter(item => item.decision === decisionVal);
        }

        // 保有株数フィルタ
        const sharesVal = sharesFilter.value;
        if (sharesVal === 'lt100') {
            filteredData = filteredData.filter(item => {
                const n = parseShares(item.shares);
                return n !== null && n < 100;
            });
        } else if (sharesVal === 'gte100') {
            filteredData = filteredData.filter(item => {
                const n = parseShares(item.shares);
                return n !== null && n >= 100;
            });
        }

        // 株主優待フィルタ
        const yuutaiVal = yuutaiFilter.value;
        if (yuutaiVal !== 'all') {
            filteredData = filteredData.filter(item => item.yuutai === yuutaiVal);
        }

        if (currentSortColumn) {
            filteredData.sort((a, b) => {
                let valA = a[currentSortColumn];
                let valB = b[currentSortColumn];
                
                const numericCols = ['code', 'per', 'pbr', 'dividend_yield',
                                     'shares', 'cost_price', 'current_price',
                                     'acquisition_cost', 'market_value', 'profit_loss', 'return_rate'];
                if (numericCols.includes(currentSortColumn)) {
                    valA = parseNumeric(valA);
                    valB = parseNumeric(valB);
                    if (valA > valB) return currentSortAsc ? 1 : -1;
                    if (valA < valB) return currentSortAsc ? -1 : 1;
                    return 0;
                } else {
                    valA = valA || "";
                    valB = valB || "";
                    return currentSortAsc ? valA.localeCompare(valB, 'ja') : valB.localeCompare(valA, 'ja');
                }
            });
        }

        resultsBody.innerHTML = ''; 
        if(filteredData.length === 0) {
            const tr = document.createElement('tr');
            tr.innerHTML = `<td colspan="16" style="text-align: center;">該当するデータが見つかりませんでした</td>`;
            resultsBody.appendChild(tr);
        } else {
            filteredData.forEach((stock, index) => {
                const tr = document.createElement('tr');
                tr.style.animation = `fadeIn 0.3s ease-out ${index * 0.02}s both`;
                
                const isFetching = stock.per === '取得中';
                const fClass = isFetching ? 'fetching-text' : '';

                // 保有株数が100未満なら「優待錠柤？」を表示
                const sharesN = parseShares(stock.shares);
                let displayDecision = stock.decision;
                let decisionBadge = 'badge-warning';
                if (sharesN !== null && sharesN < 100) {
                    displayDecision = '優待？';
                    decisionBadge = 'badge-yuutai';
                } else if (stock.decision === '保持') {
                    decisionBadge = 'badge-success';
                } else if (stock.decision === '取得中') {
                    decisionBadge = 'badge-info';
                }

                // 評価損益・騰落率の色クラス
                // 評価損益: プラス=赤、マイナス=緑
                // 騰落率: 0%以上(利益)=赤、0%未満(損失)=緑
                const plNum = parseNumeric(stock.profit_loss);
                const rrNum = parseNumeric(stock.return_rate);
                const plClass = plNum !== -Infinity ? (plNum >= 0 ? 'profit-positive' : 'profit-negative') : '';
                const rrClass = rrNum !== -Infinity ? (rrNum >= 0 ? 'profit-positive' : 'profit-negative') : '';

                tr.innerHTML = `
                    <td><strong>${stock.code}</strong></td>
                    <td>${stock.name}</td>
                    <td class="${fClass}">${stock.per}</td>
                    <td class="${fClass}">${stock.pbr}</td>
                    <td class="highlight ${fClass}">${stock.dividend_yield}</td>
                    <td><span class="badge ${decisionBadge}">${displayDecision}</span></td>
                    <td>${stock.shares || '-'}</td>
                    <td>${stock.cost_price || '-'}</td>
                    <td>${stock.current_price || '-'}</td>
                    <td>${stock.acquisition_cost || '-'}</td>
                    <td>${stock.market_value || '-'}</td>
                    <td class="${plClass}">${stock.profit_loss || '-'}</td>
                    <td class="${rrClass}">${stock.return_rate || '-'}</td>
                    <td>${stock.yuutai}</td>
                    <td>
                        <button class="btn-action" onclick="window.refetchStock('${stock.code}')" ${isFetching ? 'disabled' : ''}>
                            ${isFetching ? '取得中...' : '再取得'}
                        </button>
                    </td>
                `;
                resultsBody.appendChild(tr);
            });
        }

        loadingState.classList.add('hidden');
        resultsContainer.classList.remove('hidden');
        updateFetchProgress();
    }

    function renderResults(data) {
        portfolioData = data;
        renderFilteredAndSortedResults();
    }

    decisionFilter.addEventListener('change', renderFilteredAndSortedResults);
    sharesFilter.addEventListener('change', renderFilteredAndSortedResults);
    yuutaiFilter.addEventListener('change', renderFilteredAndSortedResults);

    document.querySelectorAll('.sortable .sort-icon').forEach(icon => icon.textContent = ' ↕');

    document.querySelectorAll('.sortable').forEach(th => {
        th.addEventListener('click', () => {
             const column = th.dataset.sort;
             if (currentSortColumn === column) {
                 currentSortAsc = !currentSortAsc;
             } else {
                 currentSortColumn = column;
                 currentSortAsc = true;
             }
             document.querySelectorAll('.sortable .sort-icon').forEach(icon => icon.textContent = ' ↕');
             th.querySelector('.sort-icon').textContent = currentSortAsc ? ' ▲' : ' ▼';
             renderFilteredAndSortedResults();
        });
    });

    // Reset workflow
    resetBtn.addEventListener('click', () => {
        portfolioData = [];
        fetchProgress.textContent = '';
        resultsContainer.classList.add('hidden');
        fileInput.value = ''; // clears the input
        dropZone.classList.remove('hidden');
    });

    window.refetchStock = async function(code) {
        const stockIndex = portfolioData.findIndex(s => s.code === code);
        if (stockIndex === -1) return;
        
        // Set state to fetching
        portfolioData[stockIndex].per = '取得中';
        portfolioData[stockIndex].pbr = '取得中';
        portfolioData[stockIndex].dividend_yield = '取得中';
        portfolioData[stockIndex].decision = '取得中';
        renderFilteredAndSortedResults();

        try {
            const response = await fetch(`/fetch/${code}?name=${encodeURIComponent(portfolioData[stockIndex].name)}`);
            if (response.ok) {
                const updatedStock = await response.json();
                // CSV由来の列は保持する（再取得では上書きしない）
                const preserved = {
                    shares: portfolioData[stockIndex].shares,
                    cost_price: portfolioData[stockIndex].cost_price,
                    current_price: portfolioData[stockIndex].current_price,
                    acquisition_cost: portfolioData[stockIndex].acquisition_cost,
                    market_value: portfolioData[stockIndex].market_value,
                    profit_loss: portfolioData[stockIndex].profit_loss,
                    return_rate: portfolioData[stockIndex].return_rate,
                };
                portfolioData[stockIndex] = { ...updatedStock, ...preserved };
            } else {
                portfolioData[stockIndex].per = 'エラー';
            }
        } catch (e) {
            portfolioData[stockIndex].per = 'エラー';
        }
        
        renderFilteredAndSortedResults();
    };

    /**
     * サイレントフェッチ：再レンダリングしない内部用関数（バッチ内で使用）
     */
    async function fetchStockSilent(code) {
        const stockIndex = portfolioData.findIndex(s => s.code === code);
        if (stockIndex === -1) return;
        
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 15000); // 15秒でタイムアウト

        try {
            const name = encodeURIComponent(portfolioData[stockIndex].name);
            const response = await fetch(`/fetch/${code}?name=${name}`, { signal: controller.signal });
            clearTimeout(timeoutId);
            
            if (response.ok) {
                const updatedStock = await response.json();
                const preserved = {
                    shares:           portfolioData[stockIndex].shares,
                    cost_price:       portfolioData[stockIndex].cost_price,
                    current_price:    portfolioData[stockIndex].current_price,
                    acquisition_cost: portfolioData[stockIndex].acquisition_cost,
                    market_value:     portfolioData[stockIndex].market_value,
                    profit_loss:      portfolioData[stockIndex].profit_loss,
                    return_rate:      portfolioData[stockIndex].return_rate,
                };
                portfolioData[stockIndex] = { ...updatedStock, ...preserved };
            } else {
                portfolioData[stockIndex].per = 'エラー';
            }
        } catch (e) {
            portfolioData[stockIndex].per = 'エラー';
        }
    }

    /**
     * 並列バッチ取得：2件ずつ同時に取得し、安定性を最優先（1秒間隔）
     */
    async function startParallelFetches() {
        const BATCH_SIZE = 2;
        const pending = portfolioData
            .filter(s => s.per === '取得中')
            .map(s => s.code);

        for (let i = 0; i < pending.length; i += BATCH_SIZE) {
            const batch = pending.slice(i, i + BATCH_SIZE);
            // バッチ内を同時取得
            await Promise.all(batch.map(code => fetchStockSilent(code)));
            // バッチ完了後に1回だけ再レンダリング
            renderFilteredAndSortedResults();
            
            // Yahoo Financeの制限を回避するため、たっぷり1000msの待機を入れる
            if (i + BATCH_SIZE < pending.length) {
                await new Promise(r => setTimeout(r, 1000));
            }
        }
    }

    // アップロード後に並列バッチ取得で開始
    function startSequentialFetches() {
        startParallelFetches();
    }
});
