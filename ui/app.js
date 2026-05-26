const API_BASE_URL = "http://localhost:8000";
const USE_MOCK_API = true;
const CSV_DATA_URL = "./befood_bachkhoa_restaurants.csv";
const MENU_CSV_DATA_URL = "./befood_bachkhoa_menu_items.csv";
const API_ENDPOINTS = {
  recommendations: "/api/recommendations/search",
  matchScore: "/api/recommendations/match-score",
  evidence: "/api/recommendations/evidence"
};

const fallbackPhotos = [
  "https://images.unsplash.com/photo-1603133872878-684f208fb84b?auto=format&fit=crop&w=700&q=80",
  "https://images.unsplash.com/photo-1562967916-eb82221dfb36?auto=format&fit=crop&w=700&q=80",
  "https://images.unsplash.com/photo-1546069901-ba9599a7e63c?auto=format&fit=crop&w=700&q=80",
  "https://images.unsplash.com/photo-1512058564366-18510be2db19?auto=format&fit=crop&w=700&q=80",
  "https://images.unsplash.com/photo-1504674900247-0877df9cc836?auto=format&fit=crop&w=700&q=80"
];

const fallbackRestaurants = [
  {
    restaurant_id: "demo-1",
    restaurant_name: "Thiên Trường - Phở Cồ & Cơm Gà Xối Mắm",
    address: "15 ngõ 75 Giải Phóng, Đồng Tâm, Đống Đa, Hà Nội",
    rating: "4.5",
    review_count: "39",
    price_min: "10000",
    price_max: "80000",
    distance_km: "0.452",
    categories_text: "PHỞ | CƠM RANG | Combo cùng coca",
    matched_terms_text: "cơm | phở",
    opening_hours: "08:00-22:00",
    image_url: fallbackPhotos[0],
    menu_count: "28",
    comment_count: "10",
    comments_list: '["Ngon xỉu, Đóng gói tốt", "Sạch sẽ", "Khá ngon"]'
  }
];

const state = {
  query: {
    food: "",
    price: "30000-50000",
    location: "Ký túc xá Đại học Bách Khoa"
  },
  allRestaurants: [],
  menuItemsByRestaurant: new Map(),
  restaurants: [],
  hasUserQuery: false
};

const form = document.querySelector("#searchForm");
const foodInput = document.querySelector("#foodInput");
const priceMinInput = document.querySelector("#priceMinInput");
const priceMaxInput = document.querySelector("#priceMaxInput");
const locationInput = document.querySelector("#locationInput");
const locationSearch = document.querySelector("#locationSearch");
const locationSearchInput = document.querySelector("#locationSearchInput");
const locationClearButton = document.querySelector("#locationClearButton");
const locationLocateButton = document.querySelector("#locationLocateButton");
const locationSuggestions = document.querySelector("#locationSuggestions");
const sortInput = document.querySelector("#sortInput");
const grid = document.querySelector("#restaurantGrid");
const resultMeta = document.querySelector("#resultMeta");
const statusBox = document.querySelector("#statusBox");
const resultsSection = document.querySelector("#results");
const modal = document.querySelector("#restaurantModal");
const modalContent = document.querySelector("#modalContent");
const locationOptions = [
  {
    name: "Đại Học Xây Dựng",
    address: "55 Giải Phóng, P.Đồng Tâm, Q.Hai Bà Trưng, Hà Nội"
  },
  {
    name: "Đại Học Bách Khoa Hà Nội",
    address: "Trần Đại Nghĩa, P.Bách Khoa, Q.Hai Bà Trưng, Hà Nội"
  },
  {
    name: "Đại Học Mở Hà Nội - Điểm Đón Trả",
    address: "B101 Nguyễn Hiền, P.Bách Khoa, Q.Hai Bà Trưng, Hà Nội"
  },
  {
    name: "Đại Học Kinh Tế Quốc Dân",
    address: "Trần Đại Nghĩa, P.Đồng Tâm, Q.Hai Bà Trưng, Hà Nội"
  },
  {
    name: "Đại Học Y Hà Nội",
    address: "1 Tôn Thất Tùng, P.Trung Tự, Q.Đống Đa, Hà Nội"
  }
];

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatCurrency(value) {
  const amount = Number(value) || 0;
  if (amount <= 0) return "Đang cập nhật";
  return new Intl.NumberFormat("vi-VN").format(amount) + "đ";
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (char === '"' && inQuotes && next === '"') {
      field += '"';
      index += 1;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === "," && !inQuotes) {
      row.push(field);
      field = "";
    } else if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") index += 1;
      row.push(field);
      if (row.some((cell) => cell.trim() !== "")) rows.push(row);
      row = [];
      field = "";
    } else {
      field += char;
    }
  }

  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }

  const headers = rows.shift() || [];
  return rows.map((cells) =>
    headers.reduce((record, header, index) => {
      record[header] = cells[index] ?? "";
      return record;
    }, {})
  );
}

function parseComments(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.filter(Boolean).slice(0, 4) : [];
  } catch {
    return value
      .replace(/^\[|\]$/g, "")
      .split('", "')
      .map((comment) => comment.replaceAll('"', "").trim())
      .filter(Boolean)
      .slice(0, 4);
  }
}

function getClosingTime(openingHours) {
  const [, close] = String(openingHours || "").split("-");
  return close || "22:00";
}

function estimateConfidence(item, index, query) {
  const searchable = `${item.name} ${item.categories} ${item.matchedTerms}`.toLowerCase();
  const terms = query.food.toLowerCase().split(/\s+/).filter(Boolean);
  const termHits = terms.filter((term) => searchable.includes(term)).length;
  const ratingScore = Math.min(item.rating || 0, 5) * 7;
  const distanceScore = Math.max(0, 24 - (item.distanceKm || 0) * 6);
  const termScore = terms.length ? (termHits / terms.length) * 30 : 18;
  const popularityScore = Math.min(item.reviews, 250) / 10;
  return Math.max(45, Math.min(97, Math.round(38 + termScore + ratingScore + distanceScore + popularityScore - index * 0.5)));
}

function buildEvidence(restaurant, query = state.query) {
  const evidence = [];
  const searchable = `${restaurant.name} ${restaurant.categories} ${restaurant.matchedTerms}`.toLowerCase();
  const queryTerms = query.food.toLowerCase().split(/\s+/).filter((term) => term.length > 1);
  const matchedTerms = queryTerms.filter((term) => searchable.includes(term));

  if (matchedTerms.length) {
    evidence.push(`Khớp món đang tìm: ${matchedTerms.join(", ")}.`);
  }

  if (restaurant.distanceKm <= 0.7) {
    evidence.push(`Rất gần khu vực đã chọn, khoảng ${restaurant.distanceKm.toFixed(1)} km.`);
  } else if (restaurant.distanceKm <= 2.5) {
    evidence.push(`Nằm trong bán kính hợp lý, khoảng ${restaurant.distanceKm.toFixed(1)} km.`);
  }

  if (restaurant.rating >= 4.5) {
    evidence.push(`Đánh giá cao ${restaurant.rating.toFixed(1)}/5 từ ${restaurant.reviews} lượt đánh giá.`);
  } else if (restaurant.reviews > 50) {
    evidence.push(`Có ${restaurant.reviews} lượt đánh giá, đủ tín hiệu phổ biến để xếp hạng.`);
  }

  if (restaurant.priceMin || restaurant.priceMax) {
    evidence.push(`Khoảng giá ${formatCurrency(restaurant.priceMin)} - ${formatCurrency(restaurant.priceMax)} phù hợp bộ lọc.`);
  }

  return evidence.slice(0, 5);
}

async function postJson(endpoint, payload) {
  const response = await fetch(`${API_BASE_URL}${endpoint}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    throw new Error(`API ${endpoint} trả về lỗi ${response.status}`);
  }

  return response.json();
}

async function fetchMatchScore(restaurant, query) {
  if (USE_MOCK_API) {
    return estimateConfidence(restaurant, 0, query);
  }

  const data = await postJson(API_ENDPOINTS.matchScore, { restaurant, query });
  return Math.round(Number(data.confidence ?? data.score ?? 0));
}

async function fetchEvidence(restaurant, query) {
  if (USE_MOCK_API) {
    return buildEvidence(restaurant, query);
  }

  const data = await postJson(API_ENDPOINTS.evidence, { restaurant, query });
  return Array.isArray(data.evidence) ? data.evidence : [];
}

async function enrichRestaurantMatchData(restaurants, query) {
  if (USE_MOCK_API) {
    return restaurants.map((restaurant, index) => ({
      ...restaurant,
      confidence: estimateConfidence(restaurant, index, query),
      evidence: buildEvidence(restaurant, query)
    }));
  }

  return Promise.all(
    restaurants.map(async (restaurant, index) => {
      try {
        const [confidence, evidence] = await Promise.all([
          fetchMatchScore(restaurant, query),
          fetchEvidence(restaurant, query)
        ]);

        return {
          ...restaurant,
          confidence: confidence || estimateConfidence(restaurant, index, query),
          evidence: evidence.length ? evidence : buildEvidence(restaurant, query)
        };
      } catch {
        return {
          ...restaurant,
          confidence: estimateConfidence(restaurant, index, query),
          evidence: buildEvidence(restaurant, query)
        };
      }
    })
  );
}

function normalizeRestaurant(row, index, query = state.query) {
  const priceMin = Number(row.price_min || row.priceMin || 0);
  const priceMax = Number(row.price_max || row.priceMax || priceMin || 0);
  const restaurant = {
    id: row.restaurant_id || row.id || `csv-${index}`,
    name: row.restaurant_name || row.name || "Quán ăn Bách Khoa",
    source: row.source || "befood",
    address: row.address || "Khu vực Bách Khoa, Hà Nội",
    latitude: Number(row.latitude || 0),
    longitude: Number(row.longitude || 0),
    rating: Number(row.rating || row.avg_rating || 0),
    reviews: Number(row.review_count || row.total_rating || 0),
    distanceKm: Number(row.distance_km || row.restaurant_distance_km || 0),
    priceMin,
    priceMax,
    categories: row.categories_text || row.merchant_category || "Nhà hàng",
    matchedTerms: row.matched_terms_text || "",
    openingHours: row.opening_hours || "",
    openUntil: getClosingTime(row.opening_hours || row.end_time),
    image: row.image_url || fallbackPhotos[index % fallbackPhotos.length],
    menuCount: Number(row.menu_count || 0),
    commentCount: Number(row.comment_count || 0),
    comments: parseComments(row.comments_list)
  };

  restaurant.confidence = estimateConfidence(restaurant, index, query);
  return restaurant;
}

async function loadCsvRestaurants() {
  if (state.allRestaurants.length) return state.allRestaurants;

  const response = await fetch(CSV_DATA_URL);
  if (!response.ok) {
    throw new Error(`Không đọc được ${CSV_DATA_URL}`);
  }

  const csvText = await response.text();
  state.allRestaurants = parseCsv(csvText).map((row, index) => normalizeRestaurant(row, index));
  return state.allRestaurants;
}

function normalizeMenuItem(row, index) {
  return {
    id: row.restaurant_item_id || `menu-${index}`,
    restaurantId: row.restaurant_id || "",
    categoryName: row.category_name || "Menu",
    name: row.item_name || "Món ăn",
    details: row.item_details || "",
    price: Number(row.price || 0),
    oldPrice: Number(row.old_price || 0),
    likeCount: Number(row.like_count || 0),
    image: row.item_image || fallbackPhotos[index % fallbackPhotos.length],
    categoryPosition: Number(row.category_position || 0),
    itemPosition: Number(row.item_position || 0)
  };
}

async function loadMenuItems() {
  if (state.menuItemsByRestaurant.size) return state.menuItemsByRestaurant;

  const response = await fetch(MENU_CSV_DATA_URL);
  if (!response.ok) {
    throw new Error(`Không đọc được ${MENU_CSV_DATA_URL}`);
  }

  const csvText = await response.text();
  const items = parseCsv(csvText).map((row, index) => normalizeMenuItem(row, index));

  items
    .sort((a, b) => a.categoryPosition - b.categoryPosition || a.itemPosition - b.itemPosition)
    .forEach((item) => {
      if (!state.menuItemsByRestaurant.has(item.restaurantId)) {
        state.menuItemsByRestaurant.set(item.restaurantId, []);
      }
      state.menuItemsByRestaurant.get(item.restaurantId).push(item);
    });

  return state.menuItemsByRestaurant;
}

function matchesQuery(restaurant, query) {
  const [minPrice, maxPrice] = query.price.split("-").map(Number);
  const text = `${restaurant.name} ${restaurant.categories} ${restaurant.matchedTerms}`.toLowerCase();
  const terms = query.food.toLowerCase().split(/\s+/).filter((term) => term.length > 1);
  const matchesFood = terms.length === 0 || terms.some((term) => text.includes(term));
  const representativePrice = restaurant.priceMin || restaurant.priceMax || 0;
  const matchesPrice = representativePrice === 0 || representativePrice <= maxPrice || restaurant.priceMin <= maxPrice;
  return matchesFood && matchesPrice && (restaurant.priceMax >= minPrice || restaurant.priceMax === 0);
}

async function searchRestaurants(query) {
  if (!USE_MOCK_API) {
    const data = await postJson(API_ENDPOINTS.recommendations, {
      term: query.food,
      price_range: query.price,
      location: query.location,
      limit: 10
    });
    const restaurants = data.restaurants || data.top_restaurants || data.results || [];
    return enrichRestaurantMatchData(
      restaurants.map((item, index) => normalizeRestaurant(item, index, query)),
      query
    );
  }

  await new Promise((resolve) => setTimeout(resolve, 350));
  const rows = await loadCsvRestaurants();
  const enrichedRows = await enrichRestaurantMatchData(rows, query);
  const filtered = enrichedRows.filter((restaurant) => matchesQuery(restaurant, query));

  return (filtered.length ? filtered : enrichedRows).slice(0, 24);
}

async function loadOfflineRestaurants() {
  await new Promise((resolve) => setTimeout(resolve, 180));
  const rows = await loadCsvRestaurants();
  return rows
    .map((restaurant, index) => ({
      ...restaurant,
      confidence: estimateConfidence(restaurant, index, state.query),
      evidence: []
    }))
    .sort((a, b) => b.confidence - a.confidence)
    .slice(0, 24);
}

function sortRestaurants(restaurants) {
  const sorted = [...restaurants];

  if (sortInput.value === "rating") sorted.sort((a, b) => b.rating - a.rating);
  if (sortInput.value === "distance") sorted.sort((a, b) => a.distanceKm - b.distanceKm);
  if (sortInput.value === "price") sorted.sort((a, b) => (a.priceMin || 999999) - (b.priceMin || 999999));
  if (sortInput.value === "match") sorted.sort((a, b) => b.confidence - a.confidence);

  return sorted;
}

function renderRestaurants() {
  const restaurants = sortRestaurants(state.restaurants);

  grid.innerHTML = restaurants
    .map((restaurant, index) => {
      const evidence = restaurant.evidence?.length ? restaurant.evidence : buildEvidence(restaurant);
      const previewEvidence = evidence[0] || "Phù hợp với món, giá và tín hiệu đánh giá hiện tại.";
      const evidenceMarkup = state.hasUserQuery
        ? `
            <details class="card-evidence">
              <summary>
                <strong>Lý do phù hợp</strong>
                <span>${escapeHtml(previewEvidence)}</span>
              </summary>
              <ul>
                ${evidence.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
              </ul>
            </details>
          `
        : "";
      const cardBodyClass = state.hasUserQuery ? "card-body" : "card-body no-evidence";

      return `
        <article class="card" data-restaurant-id="${escapeHtml(restaurant.id)}" tabindex="0" role="button" aria-label="Xem chi tiết ${escapeHtml(restaurant.name)}">
          <div class="card-media">
            <img src="${escapeHtml(restaurant.image)}" alt="${escapeHtml(restaurant.name)}" loading="lazy" />
            <span class="rank ${index < 3 ? "top" : ""}">#${index + 1}</span>
            <button class="favorite" type="button" aria-label="Lưu ${escapeHtml(restaurant.name)}">♥</button>
            <span class="match">${restaurant.confidence}% phù hợp</span>
          </div>
          <div class="${cardBodyClass}">
            <h3>${escapeHtml(restaurant.name)}</h3>
            <div class="facts">
              <span class="rating">★ ${restaurant.rating.toFixed(1)} <small>(${restaurant.reviews} đánh giá)</small></span>
            </div>
            ${evidenceMarkup}
          </div>
        </article>
      `;
    })
    .join("");

  resultMeta.innerHTML = state.hasUserQuery
    ? `Tìm thấy <strong>${restaurants.length}</strong> quán phù hợp với <strong>${escapeHtml(state.query.food)}</strong> tại <strong>${escapeHtml(state.query.location)}</strong>.`
    : `Đang hiển thị <strong>${restaurants.length}</strong> quán ăn offline gần khu vực Bách Khoa.`;
}

function renderMenuItems(menuItems) {
  if (!menuItems.length) {
    return `<p class="menu-empty">Chưa có dữ liệu menu cho quán này.</p>`;
  }

  return menuItems
    .slice(0, 12)
    .map(
      (item) => `
        <article class="menu-item">
          <img src="${escapeHtml(item.image)}" alt="${escapeHtml(item.name)}" loading="lazy" />
          <div>
            <span>${escapeHtml(item.categoryName)}</span>
            <h4>${escapeHtml(item.name)}</h4>
            ${item.details ? `<p>${escapeHtml(item.details)}</p>` : ""}
            <div class="menu-item-meta">
              <strong>${formatCurrency(item.price)}</strong>
              <small>${item.likeCount} lượt thích</small>
            </div>
          </div>
        </article>
      `
    )
    .join("");
}

async function renderRestaurantDetail(restaurant) {
  modal.classList.remove("hidden");
  document.body.classList.add("modal-open");
  modalContent.innerHTML = `<div class="detail-body"><p class="menu-empty">Đang tải menu món ăn...</p></div>`;

  const categoryTags = restaurant.categories
    .split("|")
    .map((tag) => tag.trim())
    .filter(Boolean)
    .slice(0, 8);
  const evidence = buildEvidence(restaurant);
  const detailEvidence = restaurant.evidence?.length ? restaurant.evidence : evidence;
  let menuItems = [];

  try {
    const menuMap = await loadMenuItems();
    menuItems = menuMap.get(restaurant.id) || [];
  } catch {
    menuItems = [];
  }

  const evidenceSection = state.hasUserQuery
    ? `
      <div class="detail-section evidence-section">
        <h3>Evidence cho độ phù hợp</h3>
        <p>FindEat xếp hạng quán này dựa trên các tín hiệu sau:</p>
        <ul>
          ${detailEvidence.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
        </ul>
      </div>
    `
    : "";

  modalContent.innerHTML = `
    <div class="detail-hero">
      <img src="${escapeHtml(restaurant.image)}" alt="${escapeHtml(restaurant.name)}" />
      <div class="detail-score">
        <strong>${restaurant.confidence}%</strong>
        <span>phù hợp</span>
      </div>
    </div>
    <div class="detail-body">
      <h2 id="modalTitle">${escapeHtml(restaurant.name)}</h2>
      <div class="detail-compact-meta">
        <span class="rating">★ ${restaurant.rating.toFixed(1)} <small>(${restaurant.reviews} đánh giá)</small></span>
        <span>Khoảng cách: ${restaurant.distanceKm.toFixed(1)} km</span>
        <span class="compact-match">${restaurant.confidence}% phù hợp</span>
      </div>
      <p class="detail-address">${escapeHtml(restaurant.address)}</p>

      <details class="detail-section detail-dropdown">
        <summary>
          <span>Thông tin quán</span>
        </summary>
        <div class="detail-grid">
          <span>Khoảng giá</span><strong>${formatCurrency(restaurant.priceMin)} - ${formatCurrency(restaurant.priceMax)}</strong>
          <span>Giờ mở cửa</span><strong>${escapeHtml(restaurant.openingHours || "Đang cập nhật")}</strong>
        </div>
      </details>

      ${evidenceSection}
      <div class="detail-section">
        <h3>Danh mục phù hợp</h3>
        <div class="detail-tags">
          ${categoryTags.map((tag) => `<span>${escapeHtml(tag)}</span>`).join("") || "<span>Nhà hàng</span>"}
        </div>
      </div>

      <div class="detail-section">
        <h3>Menu món ăn</h3>
        <div class="menu-list">
          ${renderMenuItems(menuItems)}
        </div>
      </div>

      <div class="detail-section">
        <h3>Bình luận nổi bật</h3>
        <div class="review-list">
          ${
            restaurant.comments.length
              ? restaurant.comments.map((comment) => `<blockquote>${escapeHtml(comment)}</blockquote>`).join("")
              : "<p>Chưa có bình luận mẫu trong dữ liệu CSV.</p>"
          }
        </div>
      </div>

      <div class="detail-actions">
        <button class="outline-button" type="button" data-close-modal>Quay lại kết quả</button>
      </div>
    </div>
  `;

}

function closeModal() {
  modal.classList.add("hidden");
  document.body.classList.remove("modal-open");
}

function setStatus(message, isVisible = true) {
  statusBox.textContent = message;
  statusBox.classList.toggle("hidden", !isVisible);
}

async function runSearch({ scrollToResults = false } = {}) {
  const priceMin = Number(priceMinInput.value || 0);
  const priceMax = Number(priceMaxInput.value || 150000);
  const currentLocation = locationInput?.value.trim() || locationSearchInput?.value.trim() || "Đại học Bách Khoa";

  state.query = {
    food: foodInput.value.trim(),
    price: `${Math.min(priceMin, priceMax)}-${Math.max(priceMin, priceMax)}`,
    location: currentLocation
  };
  state.hasUserQuery = Boolean(state.query.food);

  setStatus("Đang đọc befood_bachkhoa_restaurants.csv và tính điểm gợi ý...");
  grid.innerHTML = "";

  if (scrollToResults) {
    resultsSection.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  try {
    state.restaurants = await searchRestaurants(state.query);
    renderRestaurants();
    setStatus("Mock data đang lấy trực tiếp từ befood_bachkhoa_restaurants.csv.", USE_MOCK_API);
  } catch (error) {
    state.allRestaurants = fallbackRestaurants.map((row, index) => normalizeRestaurant(row, index));
    state.restaurants = state.allRestaurants;
    renderRestaurants();
    setStatus(`Không đọc được CSV, đã dùng fallback data. Chi tiết: ${error.message}`);
  }
}

async function initOfflineRestaurants() {
  state.hasUserQuery = false;
  state.query.food = "";
  setStatus("Đang hiển thị danh sách quán ăn offline từ befood_bachkhoa_restaurants.csv...");
  grid.innerHTML = "";

  try {
    state.restaurants = await loadOfflineRestaurants();
    renderRestaurants();
    setStatus("Chế độ offline: dữ liệu quán ăn đang lấy từ file CSV cục bộ.", true);
  } catch (error) {
    state.allRestaurants = fallbackRestaurants.map((row, index) => normalizeRestaurant(row, index));
    state.restaurants = state.allRestaurants;
    renderRestaurants();
    setStatus(`Không đọc được CSV offline, đã dùng fallback data. Chi tiết: ${error.message}`);
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  runSearch({ scrollToResults: true });
});

sortInput.addEventListener("change", renderRestaurants);

grid.addEventListener("click", (event) => {
  if (event.target.closest(".favorite")) return;
  if (event.target.closest(".card-evidence")) return;
  const card = event.target.closest("[data-restaurant-id]");
  if (!card) return;
  const restaurant = state.restaurants.find((item) => item.id === card.dataset.restaurantId);
  if (restaurant) renderRestaurantDetail(restaurant);
});

grid.addEventListener("keydown", (event) => {
  if (event.key !== "Enter" && event.key !== " ") return;
  const card = event.target.closest("[data-restaurant-id]");
  if (!card) return;
  event.preventDefault();
  const restaurant = state.restaurants.find((item) => item.id === card.dataset.restaurantId);
  if (restaurant) renderRestaurantDetail(restaurant);
});

document.querySelectorAll("[data-close-modal]").forEach((element) => {
  element.addEventListener("click", closeModal);
});

modal.addEventListener("click", (event) => {
  if (event.target.closest("[data-close-modal]")) closeModal();
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") closeModal();
});

function renderLocationSuggestions(filterText = "") {
  const keyword = filterText.trim().toLowerCase();
  const options = locationOptions.filter((option) => {
    const text = `${option.name} ${option.address}`.toLowerCase();
    return !keyword || text.includes(keyword);
  });

  locationSuggestions.innerHTML = options
    .map(
      (option) => `
        <button class="location-suggestion" type="button" data-location="${escapeHtml(`${option.name}, ${option.address}`)}">
          <span class="suggestion-pin">●</span>
          <span>
            <strong>${escapeHtml(option.name)}</strong>
            <small>${escapeHtml(option.address)}</small>
          </span>
        </button>
      `
    )
    .join("");

  locationSuggestions.classList.toggle("hidden", options.length === 0);
}

locationSearchInput?.addEventListener("focus", () => {
  renderLocationSuggestions(locationSearchInput.value);
});

locationSearchInput?.addEventListener("input", () => {
  renderLocationSuggestions(locationSearchInput.value);
});

locationSuggestions?.addEventListener("click", (event) => {
  const option = event.target.closest("[data-location]");
  if (!option) return;
  locationSearchInput.value = option.dataset.location;
  state.query.location = option.dataset.location;
  locationSuggestions.classList.add("hidden");
});

locationClearButton?.addEventListener("click", () => {
  locationSearchInput.value = "";
  renderLocationSuggestions("");
  locationSearchInput.focus();
});

locationLocateButton?.addEventListener("click", () => {
  if (!navigator.geolocation) {
    locationSearchInput.value = "Đại học Bách Khoa, Hà Nội";
    return;
  }

  navigator.geolocation.getCurrentPosition(
    () => {
      locationSearchInput.value = "Vị trí hiện tại của bạn";
      state.query.location = locationSearchInput.value;
      locationSuggestions.classList.add("hidden");
    },
    () => {
      locationSearchInput.value = "Đại học Bách Khoa, Hà Nội";
    }
  );
});

document.addEventListener("click", (event) => {
  if (!locationSearch?.contains(event.target)) {
    locationSuggestions?.classList.add("hidden");
  }
});

document.querySelectorAll("[data-food]").forEach((button) => {
  button.addEventListener("click", () => {
    foodInput.value = button.dataset.food;
    runSearch({ scrollToResults: true });
  });
});

initOfflineRestaurants();
