async function submitLogin() {
            const btn = document.getElementById('loginBtn');
            const errorBox = document.getElementById('errorMessage');
            const originalText = btn.textContent;

            const phone = document.getElementById('loginPhone').value.trim();
            const pin = document.getElementById('loginPin').value.trim();

            errorBox.style.display = 'none';
            errorBox.innerText = '';

            if(!phone || !pin) {
                showError("Please enter your phone number and PIN.");
                return;
            }
            if(pin.length !== 4 || isNaN(pin)) {
                showError("Your PIN must be exactly 4 numbers.");
                return;
            }

            btn.textContent = "Authenticating...";
            btn.disabled = true;

            const payload = { phone: phone, pin: pin };

            try {
                const response = await fetch(UzoAuth.base + '/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                if (response.ok) {
                    const data = await response.json();
                    const user = UzoAuth.save(data);
                    window.location.href = UzoAuth.destination(user);
                } else {
                    const errorData = await response.json();
                    let errorMessage = "Invalid login credentials.";

                    if (errorData.detail) {
                        if (typeof errorData.detail === 'string') {
                            errorMessage = errorData.detail;
                        } else {
                            errorMessage = JSON.stringify(errorData.detail);
                        }
                    }
                    showError(errorMessage);
                    btn.textContent = originalText;
                    btn.disabled = false;
                }
            } catch (err) {
                // THIS IS THE CATCH BLOCK THAT WAS MISSING!
                showError(err.message || "Connection error to UzoAgro Cloud. Please try again.");
                btn.textContent = originalText;
                btn.disabled = false;
            }
        }

        function showError(message) {
            const errorBox = document.getElementById('errorMessage');
            errorBox.innerText = message;
            errorBox.style.display = 'block';
        }

document.getElementById('pinVisibility').addEventListener('click', function () {
    const input = document.getElementById('loginPin');
    const visible = input.type === 'password';
    input.type = visible ? 'text' : 'password';
    this.setAttribute('aria-label', visible ? 'Hide PIN' : 'Show PIN');
    this.setAttribute('aria-pressed', String(visible));
});
