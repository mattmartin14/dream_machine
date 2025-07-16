document.addEventListener('DOMContentLoaded', () => {
    const suits = ['♥', '♦', '♣', '♠'];
    const values = ['A', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K'];
    let deck = [];

    const stockPile = document.querySelector('.stock');
    const wastePile = document.querySelector('.waste');
    const foundationPiles = document.querySelectorAll('.foundation');
    const tableauPiles = document.querySelectorAll('.tableau');
    const newGameBtn = document.getElementById('new-game-btn');

    function createDeck() {
        deck = [];
        for (let suit of suits) {
            for (let value of values) {
                deck.push({ suit, value });
            }
        }
        return deck;
    }

    function shuffleDeck() {
        for (let i = deck.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [deck[i], deck[j]] = [deck[j], deck[i]];
        }
    }

    function dealCards() {
        // Clear all piles
        [stockPile, wastePile, ...foundationPiles, ...tableauPiles].forEach(p => p.innerHTML = '');

        // Deal to tableau
        let cardIndex = 0;
        for (let i = 0; i < tableauPiles.length; i++) {
            for (let j = 0; j <= i; j++) {
                const card = createCardElement(deck[cardIndex++]);
                if (j !== i) {
                    card.classList.add('back');
                }
                card.style.top = `${j * 20}px`;
                tableauPiles[i].appendChild(card);
            }
        }

        // Add remaining cards to stock
        while (cardIndex < deck.length) {
            const card = createCardElement(deck[cardIndex++]);
            card.classList.add('back');
            stockPile.appendChild(card);
        }
    }

    function createCardElement(card) {
        const cardEl = document.createElement('div');
        cardEl.classList.add('card');
        cardEl.dataset.value = card.value;
        cardEl.dataset.suit = card.suit;
        cardEl.draggable = true;
        if (card.suit === '♥' || card.suit === '♦') {
            cardEl.classList.add('red');
        }
        if (card.suit === '♣' || card.suit === '♠') {
            cardEl.classList.add('black');
        }
        cardEl.textContent = `${card.value}${card.suit}`;
        return cardEl;
    }

    function startGame() {
        createDeck();
        shuffleDeck();
        dealCards();
    }

    newGameBtn.addEventListener('click', startGame);

    // Initial game start
    startGame();

    // Game Logic
    let draggedCard = null;

    stockPile.addEventListener('click', () => {
        if (stockPile.children.length > 0) {
            const card = stockPile.lastElementChild;
            card.classList.remove('back');
            wastePile.appendChild(card);
        } else {
            const wasteCards = Array.from(wastePile.children);
            wasteCards.reverse().forEach(card => {
                card.classList.add('back');
                stockPile.appendChild(card);
            });
        }
    });

    document.addEventListener('dragstart', (e) => {
        if (e.target.classList.contains('card') && !e.target.classList.contains('back')) {
            draggedCard = e.target;
        }
    });

    document.addEventListener('dragover', (e) => {
        e.preventDefault();
    });

    document.addEventListener('drop', (e) => {
        if (!draggedCard) return;

        const sourcePile = draggedCard.parentElement;
        const target = e.target;
        const targetPile = target.closest('.pile');

        if (!targetPile) return;

        let validMove = false;
        if (targetPile.classList.contains('tableau') && isValidTableauMove(draggedCard, targetPile.lastElementChild)) {
            validMove = true;
        } else if (targetPile.classList.contains('foundation') && isValidFoundationMove(draggedCard, targetPile.lastElementChild)) {
            validMove = true;
        }

        if (validMove) {
            targetPile.appendChild(draggedCard);
            if (sourcePile.classList.contains('tableau') && sourcePile.children.length > 0) {
                sourcePile.lastElementChild.classList.remove('back');
            }
        }
        draggedCard = null;
    });


    function isValidFoundationMove(card, lastCard) {
        const cardSuit = card.dataset.suit;
        const cardValueIndex = values.indexOf(card.dataset.value);

        if (!lastCard) {
            return cardValueIndex === 0; // Must be an Ace
        }

        const lastCardSuit = lastCard.dataset.suit;
        const lastCardValueIndex = values.indexOf(lastCard.dataset.value);

        return cardSuit === lastCardSuit && cardValueIndex === lastCardValueIndex + 1;
    }

    function isValidTableauMove(card, lastCard) {
        if (!lastCard) {
            return card.dataset.value === 'K'; // Only kings on empty tableau
        }

        const cardColor = getCardColor(card);
        const lastCardColor = getCardColor(lastCard);
        const cardValueIndex = values.indexOf(card.dataset.value);
        const lastCardValueIndex = values.indexOf(lastCard.dataset.value);

        return cardColor !== lastCardColor && cardValueIndex === lastCardValueIndex - 1;
    }

    function getCardColor(card) {
        return card.classList.contains('red') ? 'red' : 'black';
    }
});
